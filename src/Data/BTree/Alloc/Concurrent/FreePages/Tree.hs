{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
-- | Module describing the tree structure of the free page database.
module Data.BTree.Alloc.Concurrent.FreePages.Tree where

import Control.Monad ((>=>))

import Data.Foldable (traverse_)
import qualified Data.Map as M

import Data.BTree.Alloc.Class
import Data.BTree.Impure
import Data.BTree.Primitives
import qualified Data.BTree.Impure as Tree

-- | The main tree structure of the free page database.
--
-- The main free page database tree maps a 'TxId' to a 'FreeSubtree'.
type FreeTree = Tree TxId FreeSubtree

-- | the subtree structure of the free page database.
--
-- Just a collection of free 'PageId's.
type FreeSubtree = Tree PageId ()

-- | Replace the subtree of a certain 'TxId'.
replaceSubtree :: AllocM m
               => TxId
               -> [PageId]
               -> FreeTree
               -> m FreeTree
replaceSubtree tx pids = deleteSubtree tx >=> insertSubtree tx pids

-- | Delete the subtree of a certain 'TxId'.
--
-- The 'TxId' will not be present anymore in the free tree after this call.
deleteSubtree :: AllocM m
              => TxId
              -> FreeTree
              -> m FreeTree
deleteSubtree tx tree = lookupTree tx tree >>= \case
    Nothing -> return tree
    Just subtree -> do
        freeSubtree subtree
        deleteTree tx tree
  where
    freeSubtree (Tree _ Nothing) = return ()
    freeSubtree (Tree h (Just nid)) = freeAllNodes h nid

    freeAllNodes :: (AllocM m, Key key, Value val)
                 => Height h
                 -> NodeId h key val
                 -> m ()
    freeAllNodes h nid = readNode h nid >>= \case
        Leaf _ -> freeNode h nid
        Idx idx -> do
            let subHgt = decrHeight h
            traverse_ (freeAllNodes subHgt) idx
            freeNode h nid

-- | Insert a subtree for a certain 'TxId'.
insertSubtree :: AllocM m
              => TxId
              -> [PageId]
              -> FreeTree
              -> m FreeTree
insertSubtree tx pids tree = do
    subtree <- insertTreeMany (M.fromList $ zip pids (repeat ())) Tree.empty
    insertTree tx subtree tree
