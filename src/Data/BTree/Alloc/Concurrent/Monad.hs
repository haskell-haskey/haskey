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
import Control.Monad.State

import Data.Proxy (Proxy(..))

import Data.BTree.Alloc.Class
import Data.BTree.Alloc.Concurrent.Environment
import Data.BTree.Alloc.Concurrent.FreePages.Query
import Data.BTree.Alloc.Concurrent.Meta
import Data.BTree.Primitives
import Data.BTree.Store
import qualified Data.BTree.Store.Class as Store

-- | Monad in which page allocations can take place.
--
-- The monad has access to a 'ConcurrentMetaStoreM' back-end which manages can
-- store and retreive the corresponding metadata.
newtype ConcurrentT env hnd m a = ConcurrentT { fromConcurrentT :: StateT (env hnd) m a }
                                deriving (Functor, Applicative, Monad, MonadIO, MonadState (env hnd))

instance MonadTrans (ConcurrentT env hnd) where
    lift = ConcurrentT . lift

-- | Run the actions in an 'ConcurrentT' monad, given a reader or writer
-- environment. -}
runConcurrentT :: ConcurrentMetaStoreM hnd m => ConcurrentT env hnd m a -> env hnd -> m (a, env hnd)
runConcurrentT m = runStateT (fromConcurrentT m)

-- | Evaluate the actions in an 'ConcurrentT' monad, given a reader or writer
-- environment. -}
evalConcurrentT :: ConcurrentMetaStoreM hnd m => ConcurrentT env hnd m a -> env hnd -> m a
evalConcurrentT m env = fst <$> runConcurrentT m env

instance (ConcurrentMetaStoreM hnd m, MonadIO m) => AllocM (ConcurrentT WriterEnv hnd m) where
    nodePageSize = ConcurrentT Store.nodePageSize

    maxPageSize = ConcurrentT Store.maxPageSize

    allocNode height n = do
        hnd <- writerHnd <$> get
        nid <- getNid
        touchPage (nodeIdToPageId nid)
        lift $ putNodePage hnd height nid n
        return nid
      where
        getNid = getFreeNodeId >>= \case
            Just nid -> return nid
            Nothing -> do
                hnd <- writerHnd <$> get
                pid <- lift $ newPageId hnd
                return $! pageIdToNodeId pid

    freeNode _ nid = freePage (nodeIdToPageId nid)

instance ConcurrentMetaStoreM hnd m => AllocReaderM (ConcurrentT WriterEnv hnd m) where
    readNode height nid = ConcurrentT $ do
        hnd <- writerHnd <$> get
        getNodePage hnd height Proxy Proxy nid

instance ConcurrentMetaStoreM hnd m => AllocReaderM (ConcurrentT ReaderEnv hnd m) where
    readNode height nid = ConcurrentT $ do
        hnd <- readerHnd <$> get
        getNodePage hnd height Proxy Proxy nid
