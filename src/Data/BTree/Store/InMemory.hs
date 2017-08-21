{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | Binary in-memory storage back-end. Can be used as a storage back-end for
-- the append-only page allocator (see "Data.BTree.Alloc").
module Data.BTree.Store.InMemory (
  -- * Storage
  Page(..)
, MemoryFile
, MemoryFiles
, MemoryStoreConfig(..)
, defMemoryStoreConfig
, MemoryStoreT
, runMemoryStoreT
, evalMemoryStoreT
, execMemoryStoreT
, emptyMemoryStore

  -- * Exceptions
, FileNotFoundError(..)
, PageNotFoundError(..)
, WrongNodeTypeError(..)
, WrongOverflowValueError(..)
) where

import Control.Applicative (Applicative, (<$>))
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.Reader
import Control.Monad.State.Class
import Control.Monad.Trans.State.Strict ( StateT, evalStateT, execStateT , runStateT)

import Data.ByteString (ByteString)
import Data.ByteString.Lazy (toStrict)
import Data.Coerce
import Data.Map (Map)
import Data.Typeable (Typeable)
import qualified Data.Map as M

import Data.BTree.Alloc.Concurrent
import Data.BTree.Impure.Structures
import Data.BTree.Primitives
import Data.BTree.Store.Class
import Data.BTree.Store.Page
import Data.BTree.Utils.Monad.Catch (justErrM)

--------------------------------------------------------------------------------

-- | A file containing a collection of pages.
type MemoryFile = Map PageId ByteString

-- | A collection of 'File's, each associated with a certain @fp@ handle.
type MemoryFiles fp = Map fp MemoryFile

lookupFile :: (MonadThrow m, Ord fp, Show fp, Typeable fp)
           => fp -> MemoryFiles fp -> m MemoryFile
lookupFile fp m = justErrM (FileNotFoundError fp) $ M.lookup fp m

lookupPage :: (Functor m, MonadThrow m, Ord fp, Show fp, Typeable fp)
           => fp -> PageId -> MemoryFiles fp -> m ByteString
lookupPage fp pid m = M.lookup pid <$> lookupFile fp m
                  >>= justErrM (PageNotFoundError fp pid)

-- | Monad in which binary storage operations can take place.
--
--  Two important instances are 'StoreM' making it a storage back-end, and
--  'ConcurrentMetaStoreM' making it a storage back-end compatible with the
--  concurrent page allocator.
newtype MemoryStoreT fp m a = MemoryStoreT
    { fromMemoryStoreT :: ReaderT MemoryStoreConfig (StateT (MemoryFiles fp) m) a
    } deriving (Applicative, Functor, Monad,
                MonadIO, MonadThrow, MonadCatch, MonadMask,
                MonadReader MemoryStoreConfig, MonadState (MemoryFiles fp))

-- | Memory store configuration.
--
-- The default configuration can be obtained by using 'defMemoryStoreConfig'.
newtype MemoryStoreConfig = MemoryStoreConfig {
    memoryStoreConfigPageSize :: PageSize
  } deriving (Show)

-- | The default configuration.
--
-- The default page size is 4096 bytes.
defMemoryStoreConfig :: MemoryStoreConfig
defMemoryStoreConfig = MemoryStoreConfig 4096

-- | Run the storage operations in the 'MemoryStoreT' monad, given a collection of
-- 'File's.
runMemoryStoreT :: MemoryStoreT fp m a    -- ^ Action to run
                -> MemoryStoreConfig      -- ^ Configuration
                -> MemoryFiles fp         -- ^ Data
                -> m (a, MemoryFiles fp)
runMemoryStoreT m config = runStateT (runReaderT (fromMemoryStoreT m) config)

-- | Evaluate the storage operations in the 'MemoryStoreT' monad, given a colletion
-- of 'File's.
evalMemoryStoreT :: Monad m
                 => MemoryStoreT fp m a -- ^ Action to run
                 -> MemoryStoreConfig   -- ^ Configuration
                 -> MemoryFiles fp      -- ^ Data
                 -> m a
evalMemoryStoreT m config = evalStateT (runReaderT (fromMemoryStoreT m) config)

-- | Execute the storage operations in the 'MemoryStoreT' monad, given a colletion of
-- 'File's.
execMemoryStoreT :: Monad m
                 => MemoryStoreT fp m a -- ^ Action to run
                 -> MemoryStoreConfig   -- ^ Configuration
                 -> MemoryFiles fp      -- ^ Data
                 -> m (MemoryFiles fp)
execMemoryStoreT m config = execStateT (runReaderT (fromMemoryStoreT m) config)

-- | Construct a store with an empty database with name of type @hnd@.
emptyMemoryStore :: MemoryFiles hnd
emptyMemoryStore = M.empty

--------------------------------------------------------------------------------

instance (Applicative m, Monad m, MonadThrow m,
          Ord fp, Show fp, Typeable fp) =>
    StoreM fp (MemoryStoreT fp m)
  where
    openHandle fp =
        modify $ M.insertWith (flip const) fp M.empty

    flushHandle _ = return ()

    closeHandle _ = return ()

    removeHandle fp =
        modify $ M.delete fp

    nodePageSize = return encodedPageSize

    maxPageSize = asks memoryStoreConfigPageSize

    getNodePage hnd h key val nid = do
        bs <- get >>= lookupPage hnd (nodeIdToPageId nid)
        case viewHeight h of
            UZero -> decodeM (leafNodePage h key val) bs >>= \case
                LeafNodePage heightSrc n ->
                    justErrM WrongNodeTypeError $ castNode heightSrc h n
            USucc _ -> decodeM (indexNodePage h key val) bs >>= \case
                IndexNodePage heightSrc n ->
                    justErrM WrongNodeTypeError $ castNode heightSrc h n

    putNodePage hnd height nid node =
        modify $ M.update (Just . M.insert (nodeIdToPageId nid) pg) hnd
      where
        pg = case viewHeight height of
            UZero -> toStrict . encode $ LeafNodePage height node
            USucc _ -> toStrict . encode $ IndexNodePage height node

    getOverflow hnd val = do
        bs <- get >>= lookupPage hnd 0
        decodeM (overflowPage val) bs >>= \case
            OverflowPage v -> justErrM WrongOverflowValueError $ castValue v

    putOverflow hnd val =
        modify $ M.update (Just . M.insert 0 pg) hnd
      where
        pg = toStrict . encode $ OverflowPage val

    listOverflows _ = gets M.keys

--------------------------------------------------------------------------------

instance (Applicative m, Monad m, MonadThrow m) =>
    ConcurrentMetaStoreM (MemoryStoreT FilePath m)
  where
    putConcurrentMeta h meta =
        modify $ M.update (Just . M.insert 0 pg) h
      where
        pg = toStrict . encode $ ConcurrentMetaPage meta

    readConcurrentMeta hnd k v = do
        Just bs <- MemoryStoreT $ gets (M.lookup hnd >=> M.lookup 0)
        decodeM (concurrentMetaPage k v) bs >>= \case
            ConcurrentMetaPage meta -> return . Just $! coerce meta

--------------------------------------------------------------------------------

-- | Exception thrown when a file is accessed that doesn't exist.
newtype FileNotFoundError hnd = FileNotFoundError hnd deriving (Show, Typeable)

instance (Typeable hnd, Show hnd) => Exception (FileNotFoundError hnd) where

-- | Exception thrown when a page that is accessed doesn't exist.
data PageNotFoundError hnd = PageNotFoundError hnd PageId deriving (Show, Typeable)

instance (Typeable hnd, Show hnd) => Exception (PageNotFoundError hnd) where

-- | Exception thrown when a node cannot be cast to the right type.
--
-- As used in 'getNodePage'.
data WrongNodeTypeError = WrongNodeTypeError deriving (Show, Typeable)

instance Exception WrongNodeTypeError where

-- | Exception thrown when a value from an overflow page cannot be cast.
--
-- As used in 'getOverflow'.
data WrongOverflowValueError = WrongOverflowValueError deriving (Show, Typeable)

instance Exception WrongOverflowValueError where

--------------------------------------------------------------------------------

