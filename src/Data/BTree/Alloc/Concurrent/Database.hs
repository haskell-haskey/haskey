{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
-- | This module implements data structures and functions related to the database.
module Data.BTree.Alloc.Concurrent.Database where

import Control.Applicative ((<$>))
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Monad (void)
import Control.Monad.IO.Class

import Data.Proxy (Proxy(..))
import qualified Data.Set as S

import STMContainers.Map (Map)
import qualified STMContainers.Map as Map

import Data.BTree.Alloc.Class
import Data.BTree.Alloc.Concurrent.Environment
import Data.BTree.Alloc.Concurrent.Meta
import Data.BTree.Alloc.Concurrent.Monad
import Data.BTree.Alloc.Concurrent.FreePages.Save
import Data.BTree.Alloc.Concurrent.FreePages.Tree
import Data.BTree.Alloc.Transaction
import Data.BTree.Impure
import Data.BTree.Primitives
import Data.BTree.Store
import qualified Data.BTree.Utils.STM.Map as Map

-- | An active concurrent database.
--
-- This can be shared amongst threads.
data ConcurrentDb hnd k v = ConcurrentDb
    { concurrentDbHandles :: ConcurrentHandles hnd
    , concurrentDbWriterLock :: MVar ()
    , concurrentDbCurrentMeta :: TVar CurrentMetaPage
    , concurrentDbMeta1 :: TVar (ConcurrentMeta k v)
    , concurrentDbMeta2 :: TVar (ConcurrentMeta k v)
    , concurrentDbReaders :: Map TxId Integer
    }

-- | All necessary database handles.
data ConcurrentHandles hnd = ConcurrentHandles {
    concurrentHandlesMain :: hnd
  , concurrentHandlesMetadata1 :: hnd
  , concurrentHandlesMetadata2 :: hnd
  } deriving (Show)

-- | Open all concurrent handles.
openConcurrentHandles :: ConcurrentMetaStoreM hnd m
                      => ConcurrentHandles hnd -> m ()
openConcurrentHandles ConcurrentHandles{..} = do
    openHandle concurrentHandlesMain
    openHandle concurrentHandlesMetadata1
    openHandle concurrentHandlesMetadata2

-- | Open a new concurrent database, with the given handles.
--
-- The handles should already have been opened using 'openConcurrentHandles'.
createConcurrentDb :: (Key k, Value v, MonadIO m, ConcurrentMetaStoreM hnd m)
                   => ConcurrentHandles hnd -> m (ConcurrentDb hnd k v)
createConcurrentDb hnds = do
    db <- newConcurrentDb hnds meta0
    setCurrentMeta meta0 db
    setCurrentMeta meta0 db
    return db
  where
    meta0 = ConcurrentMeta { concurrentMetaRevision = 0
                           , concurrentMetaTree = Tree zeroHeight Nothing
                           , concurrentMetaFreeTree = Tree zeroHeight Nothing
                           }

-- | Open the an existing database, with the given handles.
--
-- The handles should already have been opened using 'openConcurrentHandles'.
openConcurrentDb :: (Key k, Value v, MonadIO m, ConcurrentMetaStoreM hnd m)
                 => ConcurrentHandles hnd -> m (Maybe (ConcurrentDb hnd k v))
openConcurrentDb hnds@ConcurrentHandles{..} = do
    m1 <- readConcurrentMeta concurrentHandlesMetadata1 Proxy Proxy
    m2 <- readConcurrentMeta concurrentHandlesMetadata2 Proxy Proxy
    case (m1, m2) of
        (Nothing, Nothing) -> return Nothing
        (Just m , Nothing) -> Just <$> newConcurrentDb hnds m
        (Nothing, Just m ) -> Just <$> newConcurrentDb hnds m
        (Just x , Just y ) -> if concurrentMetaRevision x > concurrentMetaRevision y
                                  then Just <$> newConcurrentDb hnds x
                                  else Just <$> newConcurrentDb hnds y

-- | Close the handles of the database.
closeConcurrentHandles :: (MonadIO m, ConcurrentMetaStoreM hnd m)
                       => ConcurrentHandles hnd
                       -> m ()
closeConcurrentHandles ConcurrentHandles{..} = do
    closeHandle concurrentHandlesMain
    closeHandle concurrentHandlesMetadata1
    closeHandle concurrentHandlesMetadata2

-- | Create a new concurrent database with handles and metadata provided.
newConcurrentDb :: (Key k, Value v, MonadIO m)
                => ConcurrentHandles hnd
                -> ConcurrentMeta k v
                -> m (ConcurrentDb hnd k v)
newConcurrentDb hnds meta0 = do
    readers <- liftIO Map.newIO
    meta    <- liftIO $ newTVarIO Meta1
    lock    <- liftIO $ newMVar ()
    meta1   <- liftIO $ newTVarIO meta0
    meta2   <- liftIO $ newTVarIO meta0
    return $! ConcurrentDb
        { concurrentDbHandles = hnds
        , concurrentDbWriterLock = lock
        , concurrentDbCurrentMeta = meta
        , concurrentDbMeta1 = meta1
        , concurrentDbMeta2 = meta2
        , concurrentDbReaders = readers
        }

-- | Get the current meta data.
getCurrentMeta :: (Key k, Value v)
               => ConcurrentDb hnd k v -> STM (ConcurrentMeta k v)
getCurrentMeta db
    | ConcurrentDb { concurrentDbCurrentMeta = v } <- db
    = readTVar v >>= \case
        Meta1 -> readTVar $ concurrentDbMeta1 db
        Meta2 -> readTVar $ concurrentDbMeta2 db

-- | Write the new metadata, and switch the pointer to the current one.
setCurrentMeta :: (MonadIO m, ConcurrentMetaStoreM hnd m, Key k, Value v)
               => ConcurrentMeta k v -> ConcurrentDb hnd k v -> m ()
setCurrentMeta new db
    | ConcurrentDb
      { concurrentDbCurrentMeta = v
      , concurrentDbHandles = hnds
      } <- db
    = liftIO (atomically $ readTVar v) >>= \case
        Meta1 -> do
            putConcurrentMeta (concurrentHandlesMetadata2 hnds) new
            liftIO . atomically $ do
                writeTVar v Meta2
                writeTVar (concurrentDbMeta2 db) new
        Meta2 -> do
            putConcurrentMeta (concurrentHandlesMetadata1 hnds) new
            liftIO . atomically $ do
                writeTVar v Meta1
                writeTVar (concurrentDbMeta1 db) new

{-| Execute a write transaction, with a result. -}
transact :: (MonadIO m, ConcurrentMetaStoreM hnd m, Key key, Value val)
         => (forall n. AllocM n => Tree key val -> n (Transaction key val a))
         -> ConcurrentDb hnd key val -> m a
transact act db
    | ConcurrentDb
      { concurrentDbHandles = hnds
      , concurrentDbWriterLock = lock
      , concurrentDbReaders = readers
      } <- db
    , ConcurrentHandles
      { concurrentHandlesMain = hnd
      } <- hnds
    = withLock lock $
    do
    meta <- liftIO . atomically $ getCurrentMeta db
    let newRevision = concurrentMetaRevision meta + 1
    let wEnv = WriterEnv { writerHnd = hnd
                         , writerTxId = newRevision
                         , writerReaders = readers
                         , writerNewlyFreedPages = []
                         , writerAllocdPages = S.empty
                         , writerFreedDirtyPages = []
                         , writerFreeTree = concurrentMetaFreeTree meta
                         , writerReuseablePages = []
                         , writerReuseablePagesTxId = Nothing
                         , writerReusablePagesOn = True }
    (tx, env) <- runConcurrentT (act $ concurrentMetaTree meta) wEnv
    case tx of
        Abort v -> return v
        Commit newTree v -> do
            -- Save the free'd pages to the free page database
            -- don't try to use pages from the free database when doing so
            freeTree' <- saveFreePages' (env { writerReusablePagesOn = False })

            -- Commit
            let newMeta = ConcurrentMeta
                    { concurrentMetaRevision = newRevision
                    , concurrentMetaTree     = newTree
                    , concurrentMetaFreeTree = freeTree'
                    }
            setCurrentMeta newMeta db
            return v
  where
    withLock l action = do
        () <- liftIO (takeMVar l)
        v  <- action
        liftIO (putMVar l ())
        return v

    saveFreePages' :: (MonadIO m, ConcurrentMetaStoreM hnd m)
                   => WriterEnv hnd
                   -> m FreeTree
    saveFreePages' toFree = do
        let env      = toFree { writerNewlyFreedPages = [] }
            freeTree = writerFreeTree toFree

        (freeTree', env') <- runConcurrentT (saveFreePages toFree freeTree) env

        -- Did we free any new pages? We have to put them in the free tree!
        -- Did we free any new dirty pages? We have to put them in the free tree!
        if null (writerNewlyFreedPages env') &&
           writerFreedDirtyPages toFree == writerFreedDirtyPages env'
           then return freeTree'
           else do
               let xs = writerNewlyFreedPages env' ++ writerNewlyFreedPages toFree
               saveFreePages' $ env' { writerNewlyFreedPages = xs
                                     , writerFreeTree        = freeTree' }

{-| Execute a write transaction, without a result. -}
transact_ :: (MonadIO m, ConcurrentMetaStoreM hnd m, Key key, Value val)
          => (forall n. AllocM n => Tree key val -> n (Transaction key val ()))
          -> ConcurrentDb hnd key val -> m ()
transact_ act db = void $ transact act db

{-| Execute a read-only transaction. -}
transactReadOnly :: (MonadIO m, ConcurrentMetaStoreM hnd m, Key key, Value val)
                 => (forall n. AllocReaderM n => Tree key val -> n a)
                 -> ConcurrentDb hnd key val -> m a
transactReadOnly act db
    | ConcurrentDb
      { concurrentDbHandles = hnds
      , concurrentDbReaders = readers
      } <- db
    , ConcurrentHandles
      { concurrentHandlesMain = hnd
      } <- hnds
    = do
    meta <- liftIO . atomically $ do
        meta <-getCurrentMeta db
        Map.alter (concurrentMetaRevision meta) addOne readers
        return meta
    v <- evalConcurrentT (act $ concurrentMetaTree meta) (ReaderEnv hnd)
    liftIO . atomically $ Map.alter (concurrentMetaRevision meta) subOne readers
    return v
  where
    addOne Nothing = Just 1
    addOne (Just x) = Just $! x + 1
    subOne Nothing = Nothing
    subOne (Just 0) = Nothing
    subOne (Just x) = Just $! x - 1
