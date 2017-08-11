{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
-- | This module implements data structures and functions related to the database.
module Data.BTree.Alloc.Concurrent.Database where

import Control.Applicative ((<$>))
import Control.Concurrent.STM
import Control.Monad (void, unless)
import Control.Monad.IO.Class
import Control.Monad.Catch (MonadMask)
import Control.Monad.State
import Control.Monad.Trans (lift)

import Data.Proxy (Proxy(..))
import Data.List.NonEmpty (NonEmpty((:|)))
import qualified Data.Set as S

import STMContainers.Map (Map)
import qualified STMContainers.Map as Map

import Data.BTree.Alloc.Class
import Data.BTree.Alloc.Concurrent.Environment
import Data.BTree.Alloc.Concurrent.FreePages.Save
import Data.BTree.Alloc.Concurrent.Meta
import Data.BTree.Alloc.Concurrent.Monad
import Data.BTree.Alloc.Concurrent.Overflow
import Data.BTree.Alloc.Transaction
import Data.BTree.Impure
import Data.BTree.Primitives
import Data.BTree.Store
import Data.BTree.Utils.RLock
import qualified Data.BTree.Utils.STM.Map as Map

-- | An active concurrent database.
--
-- This can be shared amongst threads.
data ConcurrentDb k v = ConcurrentDb
    { concurrentDbHandles :: ConcurrentHandles
    , concurrentDbWriterLock :: RLock
    , concurrentDbCurrentMeta :: TVar CurrentMetaPage
    , concurrentDbMeta1 :: TVar (ConcurrentMeta k v)
    , concurrentDbMeta2 :: TVar (ConcurrentMeta k v)
    , concurrentDbReaders :: Map TxId Integer
    }

-- | Open all concurrent handles.
openConcurrentHandles :: ConcurrentMetaStoreM m
                      => ConcurrentHandles -> m ()
openConcurrentHandles ConcurrentHandles{..} = do
    openHandle concurrentHandlesMain
    openHandle concurrentHandlesMetadata1
    openHandle concurrentHandlesMetadata2

-- | Open a new concurrent database, with the given handles.
--
-- The handles should already have been opened using 'openConcurrentHandles'.
createConcurrentDb :: (Key k, Value v, MonadIO m, ConcurrentMetaStoreM m)
                   => ConcurrentHandles -> m (ConcurrentDb k v)
createConcurrentDb hnds = do
    db <- newConcurrentDb hnds meta0
    setCurrentMeta meta0 db
    setCurrentMeta meta0 db
    return db
  where
    meta0 = ConcurrentMeta { concurrentMetaRevision = 0
                           , concurrentMetaNumPages = 0
                           , concurrentMetaTree = Tree zeroHeight Nothing
                           , concurrentMetaFreeTree = Tree zeroHeight Nothing
                           , concurrentMetaOverflowTree = Tree zeroHeight Nothing
                           , concurrentMetaFreshUnusedPages = S.empty
                           }

-- | Open the an existing database, with the given handles.
--
-- The handles should already have been opened using 'openConcurrentHandles'.
openConcurrentDb :: (Key k, Value v, MonadIO m, ConcurrentMetaStoreM m)
                 => ConcurrentHandles -> m (Maybe (ConcurrentDb k v))
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
closeConcurrentHandles :: (MonadIO m, ConcurrentMetaStoreM m)
                       => ConcurrentHandles
                       -> m ()
closeConcurrentHandles ConcurrentHandles{..} = do
    closeHandle concurrentHandlesMain
    closeHandle concurrentHandlesMetadata1
    closeHandle concurrentHandlesMetadata2

-- | Create a new concurrent database with handles and metadata provided.
newConcurrentDb :: (Key k, Value v, MonadIO m)
                => ConcurrentHandles
                -> ConcurrentMeta k v
                -> m (ConcurrentDb k v)
newConcurrentDb hnds meta0 = do
    readers <- liftIO Map.newIO
    meta    <- liftIO $ newTVarIO Meta1
    lock    <- liftIO   newRLock
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
               => ConcurrentDb k v -> STM (ConcurrentMeta k v)
getCurrentMeta db
    | ConcurrentDb { concurrentDbCurrentMeta = v } <- db
    = readTVar v >>= \case
        Meta1 -> readTVar $ concurrentDbMeta1 db
        Meta2 -> readTVar $ concurrentDbMeta2 db

-- | Write the new metadata, and switch the pointer to the current one.
setCurrentMeta :: (MonadIO m, ConcurrentMetaStoreM m, Key k, Value v)
               => ConcurrentMeta k v -> ConcurrentDb k v -> m ()
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

-- | Execute a write transaction, with a result.
transact :: (MonadIO m, MonadMask m, ConcurrentMetaStoreM m, Key key, Value val)
         => (forall n. AllocM n => Tree key val -> n (Transaction key val a))
         -> ConcurrentDb key val -> m a
transact act db = withRLock (concurrentDbWriterLock db) $ do
    cleanup
    transactNow act db
  where
    cleanup :: (MonadIO m, MonadMask m, ConcurrentMetaStoreM m) => m ()
    cleanup = actAndCommit db $ \meta -> do
        v <- deleteOutdatedOverflowIds (concurrentMetaOverflowTree meta)
        case v of
            Nothing -> return (Nothing, ())
            Just tree -> do
                let meta' = meta { concurrentMetaOverflowTree = tree }
                return (Just meta', ())

-- | Execute a write transaction, without cleaning up old overflow pages.
transactNow :: (MonadIO m, MonadMask m, ConcurrentMetaStoreM m, Key k, Value v)
            => (forall n. AllocM n => Tree k v -> n (Transaction k v a))
            -> ConcurrentDb k v -> m a
transactNow act db = withRLock (concurrentDbWriterLock db) $
    actAndCommit db $ \meta -> do
        tx <- act (concurrentMetaTree meta)
        case tx of
            Abort v -> return (Nothing, v)
            Commit tree v ->
                let meta' = meta { concurrentMetaTree = tree } in
                return (Just meta', v)

-- | Execute a write transaction, without a result.
transact_ :: (MonadIO m, MonadMask m, ConcurrentMetaStoreM m, Key k, Value v)
          => (forall n. AllocM n => Tree k v -> n (Transaction k v ()))
          -> ConcurrentDb k v -> m ()
transact_ act db = void $ transact act db

-- | Execute a read-only transaction.
transactReadOnly :: (MonadIO m, ConcurrentMetaStoreM m, Key key, Value val)
                 => (forall n. AllocReaderM n => Tree key val -> n a)
                 -> ConcurrentDb key val -> m a
transactReadOnly act db
    | ConcurrentDb
      { concurrentDbHandles = hnds
      , concurrentDbReaders = readers
      } <- db
    = do
    meta <- liftIO . atomically $ do
        meta <- getCurrentMeta db
        Map.alter (concurrentMetaRevision meta) addOne readers
        return meta
    v <- evalConcurrentT (act $ concurrentMetaTree meta) (ReaderEnv hnds)
    liftIO . atomically $ Map.alter (concurrentMetaRevision meta) subOne readers
    return v
  where
    addOne Nothing = Just 1
    addOne (Just x) = Just $! x + 1
    subOne Nothing = Nothing
    subOne (Just 0) = Nothing
    subOne (Just x) = Just $! x - 1

--------------------------------------------------------------------------------

-- | Run a write action that takes the current meta-data and returns new
-- meta-data to be commited, or 'Nothing' if the write transaction should be
-- aborted.
actAndCommit :: (MonadIO m, MonadMask m, ConcurrentMetaStoreM m, Key k, Value v)
             => ConcurrentDb k v
             -> (forall n. (MonadIO n, ConcurrentMetaStoreM n)
                 => ConcurrentMeta k v
                 -> ConcurrentT WriterEnv ConcurrentHandles n (Maybe (ConcurrentMeta k v), a)
                )
             -> m a
actAndCommit db act
    | ConcurrentDb
      { concurrentDbHandles = hnds
      , concurrentDbWriterLock = lock
      , concurrentDbReaders = readers
      } <- db
    = withRLock lock $ do

    meta <- liftIO . atomically $ getCurrentMeta db
    let newRevision = concurrentMetaRevision meta + 1

    ((maybeMeta, v), env) <- runConcurrentT (act meta) $
                               newWriter hnds
                                         newRevision
                                         (concurrentMetaNumPages meta)
                                         readers
                                         (concurrentMetaFreshUnusedPages meta)
                                         (concurrentMetaFreeTree meta)

    let maybeMeta' = updateMeta env <$> maybeMeta

    case maybeMeta' of
        Nothing -> do
            removeNewlyAllocatedOverflows env
            return v

        Just meta' -> do
            -- Bookkeeping
            (newMeta, _) <- flip execStateT (meta', env) $ do
                saveOverflowIds
                saveFreePages' 0
                handleFreedDirtyPages

            -- Commit
            setCurrentMeta (newMeta { concurrentMetaRevision = newRevision })
                           db
            return v

-- | Remove all overflow pages that were written in the transaction.
--
-- If the transaction is aborted, all written pages should be deleted.
removeNewlyAllocatedOverflows :: (MonadIO m, ConcurrentMetaStoreM m)
                              => WriterEnv ConcurrentHandles
                              -> m ()
removeNewlyAllocatedOverflows env = do
    let root = concurrentHandlesOverflowDir (writerHnds env)
    sequence_ [ delete root (i - 1) | i <- [1..(writerOverflowCounter env)] ]
  where
    delete root c = do
        let i = (writerTxId env, c)
        removeHandle (getOverflowHandle root i)

-- | Update the meta-data from a writer environment
updateMeta :: WriterEnv ConcurrentHandles -> ConcurrentMeta k v -> ConcurrentMeta k v
updateMeta env m = m { concurrentMetaFreeTree = writerFreeTree env }


-- | Save the newly free'd overflow pages, for deletion on the next tx.
saveOverflowIds :: (MonadIO m, ConcurrentMetaStoreM m)
                => StateT (ConcurrentMeta k v, WriterEnv ConcurrentHandles) m ()
saveOverflowIds = do
    (meta, env) <- get
    case map (\(OldOverflow i) ->i) (writerRemovedOverflows env) of
        [] -> return ()
        x:xs -> do
            (tree', env') <- lift $ flip runConcurrentT env $
                insertOverflowIds (writerTxId env)
                                  (x :| xs)
                                  (concurrentMetaOverflowTree meta)
            let meta' = (updateMeta env meta)
                            { concurrentMetaOverflowTree = tree' }
            put (meta', env')

-- | Save the free'd pages to the free page database
saveFreePages' :: (MonadIO m, ConcurrentMetaStoreM m)
               => Int
               -> StateT (ConcurrentMeta k v, WriterEnv ConcurrentHandles) m ()
saveFreePages' paranoid
    | paranoid >= 100 = error "paranoid: looping!"
    | otherwise
    = do

    -- Saving the free pages
    -- =====================
    --
    -- Saving free pages to the free database is a complicated task. At the
    -- end of a transaction we have 3 types of free pages:
    --
    --  1. 'DirtyFree': Pages that were freshly allocated from the end of
    --          the dabase file, but are no longer used. These are free'd
    --          by saving them in the metadata. They can freely be used
    --          during this routine.
    --
    --  2. 'NewlyFreed': Pages that were written by a previous transaction,
    --          but free'd in this transaction. They might still be in use
    --          by an older reader, and can thus not be used anyways.
    --
    --          Note that this list **may grow during this routine**, as
    --          new pages can be free'd.
    --
    --  3. 'OldFree': Pages that were fetched from the free database while
    --          executing the transaction. Technically, they can be used
    --          during this routine, BUT that would mean the list of
    --          'OldFree' pages can grow and shrink during the call, which
    --          would complicate the convergence/termination conditions of
    --          this routine. So currently, **we disable the use of these
    --          pages in this routine.**

    (meta, env) <- get
    (tree', env') <- lift $ runConcurrentT (saveFreePages env) $
        env { writerReusablePagesOn = False }

    let meta' = (updateMeta env meta)
                    { concurrentMetaFreeTree = tree' }
    put (meta', env' { writerFreeTree = tree' })

    -- Did we free any new pages? We have to put them in the free tree!
    unless (writerNewlyFreedPages env' == writerNewlyFreedPages env) $
       saveFreePages' (paranoid + 1)

-- | Handle the dirty pages.
--
-- Save the newly created free dirty pages to the metadata for later use.
--
-- Update the database size.
handleFreedDirtyPages :: (MonadIO m, ConcurrentMetaStoreM m)
                      => StateT (ConcurrentMeta k v, WriterEnv ConcurrentHandles) m ()
handleFreedDirtyPages = do
    (meta, env) <- get
    let meta' = meta { concurrentMetaNumPages = writerNewNumPages env
                     , concurrentMetaFreshUnusedPages = writerFreedDirtyPages env }
    put (meta', env)

--------------------------------------------------------------------------------
