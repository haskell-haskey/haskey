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
module Database.Haskey.Store.InMemory (
  -- * Storage
  Page(..)
, MemoryFile
, MemoryFiles
, MemoryStoreConfig(..)
, defMemoryStoreConfig
, memoryStoreConfigWithPageSize
, MemoryStoreT
, runMemoryStoreT
, newEmptyMemoryStore

  -- * Exceptions
, FileNotFoundError(..)
, PageNotFoundError(..)
, WrongNodeTypeError(..)
, WrongOverflowValueError(..)
) where

import Control.Applicative (Applicative, (<$>))
import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.Reader

import Data.ByteString (ByteString)
import Data.ByteString.Lazy (toStrict)
import Data.Map (Map)
import Data.Maybe (fromJust)
import Data.Typeable (Typeable, cast)
import Data.Word (Word64)
import qualified Data.Map as M

import Data.BTree.Impure.Structures
import Data.BTree.Primitives

import Database.Haskey.Alloc.Concurrent
import Database.Haskey.Store.Class
import Database.Haskey.Store.Page
import Database.Haskey.Utils.Monad.Catch (justErrM)

--------------------------------------------------------------------------------

-- | A file containing a collection of pages.
type MemoryFile = Map PageId ByteString

-- | A collection of 'File's, each associated with a certain @fp@ handle.
--
-- This is shareable amongst multiple threads.
type MemoryFiles fp = MVar (Map fp MemoryFile)

-- | Access the files.
get :: MonadIO m => MemoryStoreT fp m (Map fp MemoryFile)
get = MemoryStoreT . lift $ ask >>= liftIO . readMVar

-- | Access the files.
gets :: (Functor m, MonadIO m)
     => (Map fp MemoryFile -> a)
     -> MemoryStoreT fp m a
gets f = f <$> get

-- | Modify the files.
modify' :: MonadIO m =>
        (Map fp MemoryFile -> Map fp MemoryFile)
        -> MemoryStoreT fp m ()
modify' f = MemoryStoreT . lift $ ask >>= liftIO . flip modifyMVar_ (return . f)

lookupFile :: (MonadThrow m, Ord fp, Show fp, Typeable fp)
           => fp -> Map fp MemoryFile -> m MemoryFile
lookupFile fp m = justErrM (FileNotFoundError fp) $ M.lookup fp m

lookupPage :: (Functor m, MonadThrow m, Ord fp, Show fp, Typeable fp)
           => fp -> PageId -> Map fp MemoryFile -> m ByteString
lookupPage fp pid m = M.lookup pid <$> lookupFile fp m
                  >>= justErrM (PageNotFoundError fp pid)

-- | Monad in which binary storage operations can take place.
--
--  Two important instances are 'StoreM' making it a storage back-end, and
--  'ConcurrentMetaStoreM' making it a storage back-end compatible with the
--  concurrent page allocator.
newtype MemoryStoreT fp m a = MemoryStoreT
    { fromMemoryStoreT :: ReaderT MemoryStoreConfig (ReaderT (MemoryFiles fp) m) a
    } deriving (Applicative, Functor, Monad,
                MonadIO, MonadThrow, MonadCatch, MonadMask,
                MonadReader MemoryStoreConfig)

-- | Memory store configuration.
--
-- The default configuration can be obtained by using 'defMemoryStoreConfig'.
--
-- A configuration with a specific page size can be obtained by using
-- 'memoryStoreConfigWithPageSize'.
data MemoryStoreConfig = MemoryStoreConfig {
    memoryStoreConfigPageSize :: !PageSize
  , memoryStoreConfigMaxKeySize :: !Word64
  , memoryStoreConfigMaxValueSize :: !Word64
  } deriving (Show)

-- | The default configuration.
--
-- This is an unwrapped 'memoryStoreConfigWithPageSize' with a page size of
-- 4096.
defMemoryStoreConfig :: MemoryStoreConfig
defMemoryStoreConfig = fromJust (memoryStoreConfigWithPageSize 4096)

-- | Create a configuration with a specific page size.
--
-- The maximum key and value sizes are calculated using 'calculateMaxKeySize'
-- and 'calculateMaxValueSize'.
--
-- If the page size is too small, 'Nothing' is returned.
memoryStoreConfigWithPageSize :: PageSize -> Maybe MemoryStoreConfig
memoryStoreConfigWithPageSize pageSize
    | keySize < 8 && valueSize < 8 = Nothing
    | otherwise = Just MemoryStoreConfig {
          memoryStoreConfigPageSize = pageSize
        , memoryStoreConfigMaxKeySize = keySize
        , memoryStoreConfigMaxValueSize = valueSize }
  where
    keySize = calculateMaxKeySize pageSize (encodedPageSize zeroHeight)
    valueSize = calculateMaxValueSize pageSize keySize (encodedPageSize zeroHeight)

-- | Run the storage operations in the 'MemoryStoreT' monad, given a collection of
-- 'File's.
runMemoryStoreT :: MemoryStoreT fp m a    -- ^ Action to run
                -> MemoryStoreConfig      -- ^ Configuration
                -> MemoryFiles fp         -- ^ Data
                -> m a
runMemoryStoreT m config = runReaderT (runReaderT (fromMemoryStoreT m) config)

-- | Construct a store with an empty database with name of type @hnd@.
newEmptyMemoryStore :: IO (MemoryFiles hnd)
newEmptyMemoryStore = newMVar M.empty

--------------------------------------------------------------------------------

instance (Applicative m, Monad m, MonadIO m, MonadThrow m,
          Ord fp, Show fp, Typeable fp) =>
    StoreM fp (MemoryStoreT fp m)
  where
    openHandle fp =
        modify' $ M.insertWith (\_new old -> old) fp M.empty

    lockHandle _ = return ()

    releaseHandle _ = return ()

    flushHandle _ = return ()

    closeHandle _ = return ()

    removeHandle fp =
        modify' $ M.delete fp

    nodePageSize = return encodedPageSize
    maxPageSize = asks memoryStoreConfigPageSize
    maxKeySize = asks memoryStoreConfigMaxKeySize
    maxValueSize = asks memoryStoreConfigMaxValueSize

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
        modify' $ M.update (Just . M.insert (nodeIdToPageId nid) pg) hnd
      where
        pg = case viewHeight height of
            UZero -> toStrict . encode $ LeafNodePage height node
            USucc _ -> toStrict . encode $ IndexNodePage height node

    getOverflow hnd val = do
        bs <- get >>= lookupPage hnd 0
        decodeM (overflowPage val) bs >>= \case
            OverflowPage v -> justErrM WrongOverflowValueError $ castValue v

    putOverflow hnd val =
        modify' $ M.update (Just . M.insert 0 pg) hnd
      where
        pg = toStrict . encode $ OverflowPage val

    listOverflows _ = gets M.keys

--------------------------------------------------------------------------------

instance (Applicative m, Monad m, MonadIO m, MonadCatch m) =>
    ConcurrentMetaStoreM (MemoryStoreT FilePath m)
  where
    putConcurrentMeta h meta =
        modify' $ M.update (Just . M.insert 0 pg) h
      where
        pg = toStrict . encode $ ConcurrentMetaPage meta

    readConcurrentMeta hnd root = do
        maybeBs <- gets (M.lookup hnd >=> M.lookup 0)
        case maybeBs of
            Nothing -> return Nothing
            Just bs ->
                handle handle' (Just <$> decodeM (concurrentMetaPage root) bs) >>= \case
                    Just (ConcurrentMetaPage meta) -> return $! cast meta
                    Nothing -> return Nothing
      where
        handle' (DecodeError _) = return Nothing

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

