{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
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

import Data.Proxy (Proxy(..))

import System.FilePath ((</>))

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
    concurrentHandlesData :: FilePath
  , concurrentHandlesIndex :: FilePath
  , concurrentHandlesMetadata1 :: FilePath
  , concurrentHandlesMetadata2 :: FilePath
  , concurrentHandlesOverflowDir :: FilePath
  } deriving (Show)

-- | Construct a set of 'ConcurrentHandles' from a root directory.
concurrentHandles :: FilePath -> ConcurrentHandles
concurrentHandles fp = ConcurrentHandles {
    concurrentHandlesData        = fp </> "data" </> "data"
  , concurrentHandlesIndex       = fp </> "index" </> "index"
  , concurrentHandlesMetadata1   = fp </> "meta" </> "1"
  , concurrentHandlesMetadata2   = fp </> "meta" </> "2"
  , concurrentHandlesOverflowDir = fp </> "overflow"
  }

-- | Monad in which page allocations can take place.
--
-- The monad has access to a 'ConcurrentMetaStoreM' back-end which manages can
-- store and retreive the corresponding metadata.
newtype ConcurrentT env hnd m a = ConcurrentT { fromConcurrentT :: StateT (env hnd) m a }
                                deriving (Functor, Applicative, Monad,
                                          MonadIO, MonadThrow, MonadCatch, MonadMask,
                                          MonadState (env hnd))

instance MonadTrans (ConcurrentT env hnd) where
    lift = ConcurrentT . lift

-- | Run the actions in an 'ConcurrentT' monad, given a reader or writer
-- environment.
runConcurrentT :: ConcurrentMetaStoreM m
               => ConcurrentT env ConcurrentHandles m a
               -> env ConcurrentHandles
               -> m (a, env ConcurrentHandles)
runConcurrentT m = runStateT (fromConcurrentT m)

-- | Evaluate the actions in an 'ConcurrentT' monad, given a reader or writer
-- environment.
evalConcurrentT :: ConcurrentMetaStoreM m
                => ConcurrentT env ConcurrentHandles m a
                -> env ConcurrentHandles ->
                m a
evalConcurrentT m env = fst <$> runConcurrentT m env

instance
    (ConcurrentMetaStoreM m, MonadIO m)
    => AllocM (ConcurrentT WriterEnv ConcurrentHandles m)
  where
    nodePageSize = ConcurrentT Store.nodePageSize

    maxPageSize = ConcurrentT Store.maxPageSize

    allocNode height n = do
        hnd <- getWriterHnd height
        pid <- getAndTouchPid

        let nid = pageIdToNodeId (getSomeFreePageId pid)
        lift $ putNodePage hnd height nid n
        return nid
      where
        getAndTouchPid = getAndTouchFreePageId >>= \case
            Just pid -> return pid
            Nothing -> newTouchedPid

        getAndTouchFreePageId = case viewHeight height of
            UZero -> getFreePageId (DataState ()) >>= \case
                Nothing -> return Nothing
                Just pid -> do
                    touchPage (DataState pid)
                    return (Just pid)
            USucc _ -> getFreePageId (IndexState ()) >>= \case
                Nothing -> return Nothing
                Just pid -> do
                    touchPage (IndexState pid)
                    return (Just pid)

        newTouchedPid = case viewHeight height of
            UZero -> do
                pid <- fileStateNewNumPages . writerDataFileState <$> get
                let pid' = FreshFreePage . Fresh <$> pid
                touchPage pid'
                return $ getSValue pid'
            USucc _ -> do
                pid <- fileStateNewNumPages . writerIndexFileState <$> get
                let pid'' = FreshFreePage . Fresh <$> pid
                touchPage pid''
                return $ getSValue pid''


    freeNode height nid = case viewHeight height of
        UZero -> freePage (DataState  $ nodeIdToPageId nid)
        USucc _ -> freePage (IndexState $ nodeIdToPageId nid)

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
        hnd <- getWriterHnd height
        lift $ getNodePage hnd height Proxy Proxy nid

    readOverflow i = do
        root <- concurrentHandlesOverflowDir . writerHnds <$> get
        readOverflow' root i

instance
    ConcurrentMetaStoreM m
    => AllocReaderM (ConcurrentT ReaderEnv ConcurrentHandles m)
  where
    readNode height nid = do
        hnd <- getReaderHnd height
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

getWriterHnd :: MonadState (WriterEnv ConcurrentHandles) m
             => Height height
             -> m FilePath
getWriterHnd h = case viewHeight h of
    UZero -> gets $ concurrentHandlesData . writerHnds
    USucc _ -> gets $ concurrentHandlesIndex . writerHnds

getReaderHnd :: MonadState (ReaderEnv ConcurrentHandles) m
             => Height height
             -> m FilePath
getReaderHnd h = case viewHeight h of
    UZero -> gets $ concurrentHandlesData . readerHnds
    USucc _ -> gets $ concurrentHandlesIndex . readerHnds
