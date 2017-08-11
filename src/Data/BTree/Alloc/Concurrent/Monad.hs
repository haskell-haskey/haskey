{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE RankNTypes #-}
-- | This module implements the 'ConcurrentT' monad.
--
-- The 'ConcurrentT' monad is used to implement a page allocator with
-- concurrent readers and serialized writers.
module Data.BTree.Alloc.Concurrent.Monad where

import Control.Applicative (Applicative, (<$>))
import Control.Monad.Catch
import Control.Monad.State
import Control.Monad.Trans (lift)

import Data.Proxy (Proxy(..))

import Data.BTree.Alloc.Class
import Data.BTree.Alloc.Concurrent.Environment
import Data.BTree.Alloc.Concurrent.FreePages.Query
import Data.BTree.Alloc.Concurrent.Meta
import Data.BTree.Alloc.Concurrent.Overflow
import Data.BTree.Primitives
import Data.BTree.Store
import qualified Data.BTree.Store.Class as Store

-- | All necessary database handles.
data ConcurrentHandles = ConcurrentHandles {
    concurrentHandlesMain :: FilePath
  , concurrentHandlesMetadata1 :: FilePath
  , concurrentHandlesMetadata2 :: FilePath
  , concurrentHandlesOverflowDir :: FilePath
  } deriving (Show)

-- | Monad in which page allocations can take place.
--
-- The monad has access to a 'ConcurrentMetaStoreM' back-end which manages can
-- store and retreive the corresponding metadata.
--
-- This instance of 'MonadThrow' for 'ConcurrentT' will rewrap the exception in
-- a 'ConcurrentError' exception, along with the state when the exception is
-- thrown.
newtype ConcurrentT env hnd m a = ConcurrentT { fromConcurrentT :: StateT (env hnd) m a }
                                deriving (Functor, Applicative, Monad,
                                          MonadIO, MonadCatch, MonadMask,
                                          MonadState (env hnd))

instance MonadTrans (ConcurrentT env hnd) where
    lift = ConcurrentT . lift

instance (MonadThrow m, RecoverableState (env hnd)) => MonadThrow (ConcurrentT env hnd m) where
    throwM e = do
        s <- recover <$> get
        lift $ throwM (ConcurrentError (toException e) s)

-- | Exception that was caught while running a 'ConcurrentT' action through
-- 'runConcurrentT'. Includes the original exception and the recovered state.
data ConcurrentError = ConcurrentError SomeException (Maybe RecoveredState) deriving (Show)

instance Exception ConcurrentError where

-- | Run the actions in an 'ConcurrentT' monad, given a reader or writer
-- environment.
--
-- This function will catch all synchronous errors thrown by the
-- 'ConcurrentT' action. It will then rewrap the caught exception in a
-- 'ConcurrentError' exception along with the recovered state. The
-- 'ConcurrentError' will then be rethrown.
--
-- Asynchronous errors are not caught, and are thus not cleaned up.
--
-- See also the description of 'ConcurrentT'
runConcurrentT :: (ConcurrentMetaStoreM m, MonadMask m)
               => ConcurrentT env ConcurrentHandles m a
               -> env ConcurrentHandles
               -> m (a, env ConcurrentHandles)
runConcurrentT m = runStateT (fromConcurrentT m)

-- | Evaluate the actions in an 'ConcurrentT' monad, given a reader or writer
-- environment.
evalConcurrentT :: (ConcurrentMetaStoreM m, MonadMask m)
                => ConcurrentT env ConcurrentHandles m a
                -> env ConcurrentHandles
                -> m a
evalConcurrentT m env = fst <$> runConcurrentT m env

instance
    (ConcurrentMetaStoreM m, MonadIO m)
    => AllocM (ConcurrentT WriterEnv ConcurrentHandles m)
  where
    nodePageSize = ConcurrentT Store.nodePageSize

    maxPageSize = ConcurrentT Store.maxPageSize

    allocNode height n = do
        hnd <- concurrentHandlesMain . writerHnds <$> get
        pid <- getPid
        touchPage pid

        let nid = pageIdToNodeId (getSomeFreePageId pid)
        lift $ putNodePage hnd height nid n
        return nid
      where
        getPid = getFreePageId >>= \case
            Just pid -> return pid
            Nothing -> do
                pid <- writerNewNumPages <$> get
                return (FreshFreePage (Fresh pid))

    freeNode _ nid = freePage (nodeIdToPageId nid)

    allocOverflow v = do
        root <- concurrentHandlesOverflowDir . writerHnds <$> get
        oid <- getNewOverflowId
        touchOverflow oid

        let hnd = getOverflowHandle root oid
        lift $ openHandle hnd
        lift $ putOverflow hnd v
        lift $ closeHandle hnd
        return oid

    freeOverflow oid = overflowType oid >>= \case
        Right i -> removeOldOverflow i
        Left (DirtyOverflow i) -> do
            root <- concurrentHandlesOverflowDir . writerHnds <$> get
            lift $ removeHandle (getOverflowHandle root i)

instance
    ConcurrentMetaStoreM m
    => AllocReaderM (ConcurrentT WriterEnv ConcurrentHandles m)
  where
    readNode height nid = do
        hnd <- concurrentHandlesMain . writerHnds <$> get
        lift $ getNodePage hnd height Proxy Proxy nid

    readOverflow i = do
        root <- concurrentHandlesOverflowDir . writerHnds <$> get
        readOverflow' root i

instance
    ConcurrentMetaStoreM m
    => AllocReaderM (ConcurrentT ReaderEnv ConcurrentHandles m)
  where
    readNode height nid = do
        hnd <- concurrentHandlesMain . readerHnds <$> get
        lift $ getNodePage hnd height Proxy Proxy nid

    readOverflow i = do
        root <- concurrentHandlesOverflowDir . readerHnds <$> get
        readOverflow' root i

readOverflow' :: (ConcurrentMetaStoreM m, Value v)
              => FilePath -> OverflowId -> ConcurrentT env hnd m v
readOverflow' root oid = do
    let hnd = getOverflowHandle root oid
    lift $ openHandle hnd
    v <- lift $ getOverflow hnd Proxy
    lift $ closeHandle hnd
    return v

