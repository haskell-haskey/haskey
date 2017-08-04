{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
-- | Environments of a read or write transaction.
module Data.BTree.Alloc.Concurrent.Environment where

import Control.Monad.State

import Data.Set (Set)
import qualified Data.Set as S

import STMContainers.Map (Map)

import Data.BTree.Alloc.Concurrent.FreePages.Tree
import Data.BTree.Primitives

newtype ReaderEnv hnd = ReaderEnv { readerHnd :: hnd }

data WriterEnv hnd = WriterEnv
    { writerHnd :: !hnd
    , writerTxId :: !TxId
    , writerReaders :: Map TxId Integer

    , writerNewlyFreedPages :: ![NewlyFreed]
    -- ^ Pages free'd in this transaction, not ready for reuse until the
    -- transaction is commited.

    , writerDirtyPages :: !(Set Dirty)
    -- ^ Pages written to in this transcation. These pages can be reused in the
    -- same transaction if free'd later.

    , writerFreedDirtyPages :: ![DirtyFree]
    -- ^ Pages free for immediate reuse.
    --

    , writerFreeTree :: !FreeTree
    -- ^ The root of the free tree, might change during a transaction.
    --

    , writerReuseablePages :: ![Free]
    -- ^ Pages queried from the free pages database and ready for immediate
    -- reuse.

    , writerReuseablePagesTxId :: !(Maybe TxId)
    -- ^ The 'TxId' of the pages in 'writerReuseablePages', or 'Nothing' if no
    -- pages were queried yet from the free database.

    , writerReusablePagesOn :: !Bool
    -- ^ Used to turn of querying the free page database for free pages.
    }

-- | Create a new writer.
newWriter :: hnd -> TxId -> Map TxId Integer -> FreeTree -> WriterEnv hnd
newWriter hnd tx readers freeTree = WriterEnv {
    writerHnd = hnd
  , writerTxId = tx
  , writerReaders = readers
  , writerNewlyFreedPages = []
  , writerDirtyPages = S.empty
  , writerFreedDirtyPages = []
  , writerFreeTree = freeTree
  , writerReuseablePages = []
  , writerReuseablePagesTxId = Nothing
  , writerReusablePagesOn = True
  }

-- | Wrapper around 'PageId' indicating it is newly free'd and cannot be reused
-- in the same transaction.
newtype NewlyFreed = NewlyFreed PageId deriving (Eq, Ord, Show)

-- | Wrapper around 'PageId' indicating it is a dirty page.
newtype Dirty = Dirty PageId deriving (Eq, Ord, Show)

-- | Wrapper around 'PageId' indicating the page is dirty and free for reuse.
newtype DirtyFree = DirtyFree PageId deriving (Eq, Ord, Show)

-- | Wrapper around 'PageId' inidcating it was fetched from the free database
-- and is ready for reuse.
newtype Free = Free PageId deriving (Eq, Ord, Show)

-- | Try to free a page, given a set of dirty pages.
--
-- If the page was dirty, a 'DirtyFree' page is added to the environment, if
-- not a 'NewlyFreed' page is added to the environment.
freePage :: MonadState (WriterEnv hnd) m => PageId -> m ()
freePage pid = page >>= \case
    Right p -> modify' $ \e -> e { writerFreedDirtyPages = p : writerFreedDirtyPages e }
    Left  p -> modify' $ \e -> e { writerNewlyFreedPages = p : writerNewlyFreedPages e }
  where
    page = dirty pid >>= \case
        Just (Dirty p) -> return $ Right (DirtyFree p)
        Nothing        -> return $ Left (NewlyFreed pid)

-- | Get a 'Dirty' page, by first proving it is in fact dirty.
dirty :: MonadState (WriterEnv hnd) m => PageId -> m (Maybe Dirty)
dirty pid = (page . writerDirtyPages) <$> get
  where
    page dirty'
        | S.member (Dirty pid) dirty' = Just (Dirty pid)
        | otherwise                   = Nothing

-- | Touch a page, make it dirty.
touchPage :: MonadState (WriterEnv hnd) m => PageId -> m ()
touchPage pid = modify' $ \e -> e { writerDirtyPages = S.insert dirty' (writerDirtyPages e) }
  where dirty' = Dirty pid
