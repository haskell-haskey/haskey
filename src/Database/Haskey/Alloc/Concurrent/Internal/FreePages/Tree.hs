{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
-- | Module describing the tree structure of the free page database.
module Database.Haskey.Alloc.Concurrent.Internal.FreePages.Tree where

import Control.Monad ((>=>))

import Data.Foldable (traverse_)
import Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NE

import Data.BTree.Alloc.Class
import Data.BTree.Impure (Tree)
import Data.BTree.Impure.NonEmpty (NonEmptyTree(..))
import Data.BTree.Primitives
import qualified Data.BTree.Impure as B
import qualified Data.BTree.Impure.NonEmpty as NET

-- | The main tree structure of the free page database.
--
-- The main free page database tree maps a 'TxId' to a 'FreeSubtree'.
type FreeTree = Tree TxId FreeSubtree

-- | the subtree structure of the free page database.
--
-- Just a collection of free 'PageId's.
type FreeSubtree = NonEmptyTree PageId ()

-- | Replace the subtree of a certain 'TxId'.
replaceSubtree :: AllocM m
               => TxId
               -> NonEmpty PageId
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
deleteSubtree tx tree = B.lookup tx tree >>= \case
    Nothing -> return tree
    Just (NonEmptyTree h nid) -> do
        freeAllNodes h nid
        B.delete tx tree
  where
    freeAllNodes :: (AllocM m, Key key, Value val)
                 => Height h
                 -> NodeId h key val
                 -> m ()
    freeAllNodes h nid = readNode h nid >>= \case
        NET.Leaf _ -> freeNode h nid
        NET.Idx idx -> do
            let subHgt = decrHeight h
            traverse_ (freeAllNodes subHgt) idx
            freeNode h nid

-- | Insert a subtree for a certain 'TxId'.
insertSubtree :: AllocM m
              => TxId
              -> NonEmpty PageId
              -> FreeTree
              -> m FreeTree
insertSubtree tx pids tree = do
    subtree <- NET.fromList (NE.zip pids (NE.repeat ()))
    B.insert tx subtree tree
