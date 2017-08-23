{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
-- | Data structures and functions related to handling overflow pages.
module Database.Haskey.Alloc.Concurrent.Overflow where

import Control.Applicative ((<$>))
import Control.Concurrent.STM
import Control.Monad.State

import Data.Bits (shiftR)
import Data.Foldable (traverse_)
import Data.List.NonEmpty (NonEmpty)
import Data.Maybe (fromMaybe, listToMaybe)
import Data.Word (Word8)
import qualified Data.List.NonEmpty as NE

import Numeric (showHex, readHex)

import System.FilePath ((</>), (<.>), dropExtension, takeFileName)

import Data.BTree.Alloc.Class
import Data.BTree.Impure
import Data.BTree.Impure.NonEmpty
import Data.BTree.Primitives

import Database.Haskey.Alloc.Concurrent.Environment
import qualified Database.Haskey.Utils.STM.Map as Map

getNewOverflowId :: (Functor m, MonadState (WriterEnv hnd) m)
                 => m OverflowId
getNewOverflowId = do
    tx <- writerTxId <$> get
    c  <- writerOverflowCounter <$> get
    modify' $ \e -> e { writerOverflowCounter = 1 + writerOverflowCounter e }
    return (tx, c)

getOverflowHandle :: FilePath -> OverflowId -> FilePath
getOverflowHandle root (TxId tx, c) =
    getOverflowDir root (TxId tx) </> showHex' tx <.> showHex' c <.> "overflow"

getOverflowDir :: FilePath -> TxId -> FilePath
getOverflowDir root (TxId tx) =
    root </> lsb1 </> lsb2
  where
    lsb1 = showHex' (fromIntegral tx :: Word8)
    lsb2 = showHex' (fromIntegral (tx `shiftR` 8) :: Word8)

readOverflowId :: FilePath -> Maybe OverflowId
readOverflowId fp = parse (dropExtension $ takeFileName fp)
  where
    parse s = do
        (tx, s') <- readHex' s
        s'' <- case s' of '.':xs -> return xs
                          _      -> Nothing
        (c, _) <- readHex' s''
        return (tx, c)

showHex' :: (Integral a, Show a) => a -> String
showHex' = flip showHex ""

readHex' :: (Eq a, Num a) => String -> Maybe (a, String)
readHex' s = listToMaybe $ readHex s

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

deleteOutdatedOverflowIds :: (Functor m, AllocM m, MonadIO m,
                              MonadState (WriterEnv hnd) m)
                          => OverflowTree
                          -> m (Maybe OverflowTree)
deleteOutdatedOverflowIds tree = do
    defaultTx <- writerTxId <$> get
    readers   <- writerReaders <$> get
    oldest    <- liftIO . atomically $
        fromMaybe defaultTx <$> Map.lookupMinKey readers

    lookupMinTree tree >>= \case
        Nothing -> return Nothing
        Just (tx, _) -> if tx >= oldest
            then return Nothing
            else Just <$> go oldest tx tree
  where
    go oldest tx t = do
        t' <- deleteOverflowIds tx t
        lookupMinTree t' >>= \case
            Nothing -> return t'
            Just (tx', _) -> if tx' >= oldest
                then return t'
                else go oldest tx' t'

--------------------------------------------------------------------------------
