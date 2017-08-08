{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
-- | Data structures and functions related to handling overflow pages.
module Data.BTree.Alloc.Concurrent.Overflow where

import Control.Applicative ((<$>))
import Control.Monad.State

import Data.Bits (shiftR)
import Data.Foldable (traverse_)
import Data.List.NonEmpty (NonEmpty)
import Data.Word (Word8)
import qualified Data.List.NonEmpty as NE

import Numeric (showHex)

import System.FilePath ((</>), (<.>))

import Data.BTree.Alloc.Class
import Data.BTree.Alloc.Concurrent.Environment
import Data.BTree.Impure
import Data.BTree.Impure.NonEmpty
import Data.BTree.Primitives

getNewOverflowId :: (Functor m, MonadState (WriterEnv hnd) m)
                 => m OverflowId
getNewOverflowId = do
    tx <- writerTxId <$> get
    c  <- writerOverflowCounter <$> get
    modify' $ \e -> e { writerOverflowCounter = 1 + writerOverflowCounter e }
    return (tx, c)

getOverflowHandle :: FilePath -> OverflowId -> FilePath
getOverflowHandle root (TxId tx, c) =
    root </> lsb1 </> lsb2 </> showHex' tx <.> showHex' c <.> "overflow"
  where
    lsb1 = showHex' (fromIntegral tx :: Word8)
    lsb2 = showHex' (fromIntegral (tx `shiftR` 8) :: Word8)

    showHex' :: (Integral a, Show a) => a -> String
    showHex' = flip showHex ""

--------------------------------------------------------------------------------

-- | The main tree structure of the freed overflow page tree
type OverflowTree = Tree TxId OverflowSubtree

-- | The subtree structure of the freed overflow page tree
type OverflowSubtree = NonEmptyTree OverflowId ()

-- | Save a set of overflow ids that were free'd in the transaction.
insertOverflowIds :: AllocM m
                  => TxId
                  -> NonEmpty OverflowId
                  -> OverflowTree
                  -> m OverflowTree
insertOverflowIds tx oids tree = do
    subtree <- fromNonEmptyList (NE.zip oids (NE.repeat ()))
    insertTree tx subtree tree

-- | Delete the set of overflow ids that were free'd in the transaction.
deleteOverflowIds :: AllocM m
                  => TxId
                  -> OverflowTree
                  -> m OverflowTree
deleteOverflowIds tx tree = lookupTree tx tree >>= \case
    Nothing -> return tree
    Just (NonEmptyTree h nid) -> do
        freeAllNodes h nid
        deleteTree tx tree
  where
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

--------------------------------------------------------------------------------
