{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
-- | This module implements data structures and functions related to the database.
module Database.Haskey.Alloc.Concurrent.Database where

import Control.Applicative ((<$>))
import Control.Concurrent.STM
import Control.Monad (void, unless)
import Control.Monad.IO.Class
import Control.Monad.Catch (MonadCatch, MonadMask, SomeException,
                            catch, mask, onException, bracket)
import Control.Monad.State
import Control.Monad.Trans (lift)

import Data.Proxy (Proxy(..))
import Data.List.NonEmpty (NonEmpty((:|)))
import Data.Maybe (fromMaybe)
import qualified Data.Set as S

import STMContainers.Map (Map)
import qualified STMContainers.Map as Map

import Data.BTree.Alloc.Class
import Data.BTree.Impure
import Data.BTree.Primitives

import Database.Haskey.Alloc.Concurrent.Environment
import Database.Haskey.Alloc.Concurrent.FreePages.Save
import Database.Haskey.Alloc.Concurrent.Meta
import Database.Haskey.Alloc.Concurrent.Monad
import Database.Haskey.Alloc.Concurrent.Overflow
import Database.Haskey.Alloc.Transaction
import Database.Haskey.Store
import Database.Haskey.Utils.RLock
import qualified Database.Haskey.Utils.STM.Map as Map

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
    openHandle concurrentHandlesData
    openHandle concurrentHandlesIndex
    openHandle concurrentHandlesMetadata1
    openHandle concurrentHandlesMetadata2

-- | Open a new concurrent database, with the given handles.
createConcurrentDb :: (Key k, Value v, MonadIO m, ConcurrentMetaStoreM m)
                   => ConcurrentHandles -> m (ConcurrentDb k v)
createConcurrentDb hnds = do
    openConcurrentHandles hnds
    db <- newConcurrentDb hnds meta0
    setCurrentMeta meta0 db
    setCurrentMeta meta0 db
    return db
  where
    meta0 = ConcurrentMeta {
        concurrentMetaRevision = 0
      , concurrentMetaDataNumPages = DataState 0
      , concurrentMetaIndexNumPages = IndexState 0
      , concurrentMetaTree = Tree zeroHeight Nothing
      , concurrentMetaDataFreeTree = DataState $ Tree zeroHeight Nothing
      , concurrentMetaIndexFreeTree = IndexState $ Tree zeroHeight Nothing
      , concurrentMetaOverflowTree = Tree zeroHeight Nothing
      , concurrentMetaDataFreshUnusedPages = DataState S.empty
      , concurrentMetaIndexFreshUnusedPages = IndexState S.empty
      }

-- | Open the an existing database, with the given handles.
openConcurrentDb :: (Key k, Value v, MonadIO m, MonadMask m, ConcurrentMetaStoreM m)
                 => ConcurrentHandles -> m (Maybe (ConcurrentDb k v))
openConcurrentDb hnds@ConcurrentHandles{..} = do
    openConcurrentHandles hnds
    m1 <- readConcurrentMeta concurrentHandlesMetadata1 Proxy Proxy
    m2 <- readConcurrentMeta concurrentHandlesMetadata2 Proxy Proxy
    maybeDb <- case (m1, m2) of
        (Nothing, Nothing) -> return Nothing
        (Just m , Nothing) -> Just <$> newConcurrentDb hnds m
        (Nothing, Just m ) -> Just <$> newConcurrentDb hnds m
        (Just x , Just y ) -> if concurrentMetaRevision x > concurrentMetaRevision y
                                  then Just <$> newConcurrentDb hnds x
                                  else Just <$> newConcurrentDb hnds y
    case maybeDb of
        Nothing -> return Nothing
        Just db -> do
            meta <- liftIO . atomically $ getCurrentMeta db
            cleanupAfterException hnds (concurrentMetaRevision meta + 1)
            return (Just db)

-- | Close the handles of the database.
closeConcurrentHandles :: (MonadIO m, ConcurrentMetaStoreM m)
                       => ConcurrentHandles
                       -> m ()
closeConcurrentHandles ConcurrentHandles{..} = do
    closeHandle concurrentHandlesData
    closeHandle concurrentHandlesIndex
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
            flushHandle (concurrentHandlesData hnds)
            flushHandle (concurrentHandlesIndex hnds)
            putConcurrentMeta (concurrentHandlesMetadata2 hnds) new
            flushHandle (concurrentHandlesMetadata2 hnds)
            liftIO . atomically $ do
                writeTVar v Meta2
                writeTVar (concurrentDbMeta2 db) new
        Meta2 -> do
            flushHandle (concurrentHandlesData hnds)
            flushHandle (concurrentHandlesIndex hnds)
            putConcurrentMeta (concurrentHandlesMetadata1 hnds) new
            flushHandle (concurrentHandlesMetadata1 hnds)
            liftIO . atomically $ do
                writeTVar v Meta1
                writeTVar (concurrentDbMeta1 db) new

-- | Execute a write transaction, with a result.
transact :: (MonadIO m, MonadMask m, ConcurrentMetaStoreM m, Key key, Value val)
         => (forall n. (AllocM n, MonadMask n) => Tree key val -> n (Transaction key val a))
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
            => (forall n. (AllocM n, MonadMask n) => Tree k v -> n (Transaction k v a))
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
          => (forall n. (AllocM n, MonadMask n) => Tree k v -> n (Transaction k v ()))
          -> ConcurrentDb k v -> m ()
transact_ act db = void $ transact act db

-- | Execute a read-only transaction.
transactReadOnly :: (MonadIO m, MonadMask m, ConcurrentMetaStoreM m, Key key, Value val)
                 => (forall n. (AllocReaderM n, MonadMask m) => Tree key val -> n a)
                 -> ConcurrentDb key val -> m a
transactReadOnly act db =
    bracket acquireMeta
            releaseMeta $
            \meta -> evalConcurrentT (act $ concurrentMetaTree meta)
                                     (ReaderEnv hnds)
  where
    hnds    = concurrentDbHandles db
    readers = concurrentDbReaders db

    addOne Nothing = Just 1
    addOne (Just x) = Just $! x + 1
    subOne Nothing = Nothing
    subOne (Just 0) = Nothing
    subOne (Just x) = Just $! x - 1

    acquireMeta = liftIO . atomically $ do
        meta <- getCurrentMeta db
        Map.alter (concurrentMetaRevision meta) addOne readers
        return meta

    releaseMeta meta =
        let rev = concurrentMetaRevision meta in
        liftIO . atomically $ Map.alter rev subOne readers

--------------------------------------------------------------------------------

-- | Run a write action that takes the current meta-data and returns new
-- meta-data to be commited, or 'Nothing' if the write transaction should be
-- aborted.
actAndCommit :: (MonadIO m, MonadMask m, ConcurrentMetaStoreM m, Key k, Value v)
             => ConcurrentDb k v
             -> (forall n. (MonadIO n, MonadMask n, ConcurrentMetaStoreM n)
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
    wrap hnds newRevision $ do
        ((maybeMeta, v), env) <- runConcurrentT (act meta) $
                                   newWriter hnds
                                             newRevision
                                             readers
                                             (concurrentMetaDataNumPages meta)
                                             (concurrentMetaIndexNumPages meta)
                                             (concurrentMetaDataFreshUnusedPages meta)
                                             (concurrentMetaIndexFreshUnusedPages meta)
                                             (concurrentMetaDataFreeTree meta)
                                             (concurrentMetaIndexFreeTree meta)

        let maybeMeta' = updateMeta env <$> maybeMeta

        case maybeMeta' of
            Nothing -> do
                removeNewlyAllocatedOverflows env
                return v

            Just meta' -> do
                -- Bookkeeping
                (newMeta, _) <- flip execStateT (meta', env) $ do
                    saveOverflowIds
                    saveFreePages' 0 DataState
                                     writerDataFileState
                                     (\e s -> e { writerDataFileState = s })
                    saveFreePages' 0 IndexState
                                     writerIndexFileState
                                     (\e s -> e { writerIndexFileState = s })
                    handleFreedDirtyPages

                -- Commit
                setCurrentMeta (newMeta { concurrentMetaRevision = newRevision })
                               db
                return v
  where
    wrap :: (MonadIO m, MonadMask m, ConcurrentMetaStoreM m)
         => ConcurrentHandles
         -> TxId
         -> m a
         -> m a
    wrap hnds tx action = mask $ \restore ->
        restore action `onException` cleanupAfterException hnds tx

-- | Cleanup after an exception occurs, or after a program crash.
--
-- The 'TxId' of the aborted transaction should be passed.
cleanupAfterException :: (MonadIO m, MonadCatch m, ConcurrentMetaStoreM m)
                      => ConcurrentHandles
                      -> TxId
                      -> m ()
cleanupAfterException hnds tx = do
    let dir = getOverflowDir (concurrentHandlesOverflowDir hnds) tx
    overflows <- filter filter' <$> listOverflows dir
    mapM_ (\fp -> removeHandle fp `catch` ignore) overflows
  where
    filter' fp = fromMaybe False $ (== tx) . fst <$> readOverflowId fp

    ignore :: Monad m => SomeException -> m ()
    ignore _ = return ()

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
updateMeta env m = m {
    concurrentMetaDataFreeTree = fileStateFreeTree (writerDataFileState env)
  , concurrentMetaIndexFreeTree = fileStateFreeTree (writerIndexFileState env) }


-- | Save the newly free'd overflow pages, for deletion on the next tx.
saveOverflowIds :: (MonadIO m, MonadMask m, ConcurrentMetaStoreM m)
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
saveFreePages' :: (MonadIO m, MonadMask m, ConcurrentMetaStoreM m)
               => Int
               -> (forall a. a -> S t a)
               -> (forall hnds. WriterEnv hnds -> FileState t)
               -> (forall hnds. WriterEnv hnds -> FileState t -> WriterEnv hnds)
               -> StateT (ConcurrentMeta k v, WriterEnv ConcurrentHandles) m ()
saveFreePages' paranoid cons getState setState
    {- paranoid >= 100 = error "paranoid: looping!"
    | otherwise-}
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
    let tx = writerTxId env
    (tree', envWithoutTree) <- lift $
        runConcurrentT (saveFreePages tx (getState env)) $
            env { writerReusablePagesOn = False }

    let state' = (getState envWithoutTree) { fileStateFreeTree = cons tree' }
    let env'   = setState envWithoutTree state'
    let meta'  = updateMeta env' meta
    put (meta', env')

    -- Did we free any new pages? We have to put them in the free tree!
    unless (fileStateNewlyFreedPages state' == fileStateNewlyFreedPages (getState env)) $
       saveFreePages' (paranoid + 1) cons getState setState

-- | Handle the dirty pages.
--
-- Save the newly created free dirty pages to the metadata for later use.
--
-- Update the database size.
handleFreedDirtyPages :: (MonadIO m, MonadMask m, ConcurrentMetaStoreM m)
                      => StateT (ConcurrentMeta k v, WriterEnv ConcurrentHandles) m ()
handleFreedDirtyPages = do
    (meta, env) <- get

    let dataEnv  = writerDataFileState env
    let indexEnv = writerIndexFileState env

    let meta' = meta { concurrentMetaDataNumPages =
                            fileStateNewNumPages dataEnv
                     , concurrentMetaDataFreeTree =
                            fileStateFreeTree dataEnv
                     , concurrentMetaDataFreshUnusedPages =
                            fileStateFreedDirtyPages dataEnv

                     , concurrentMetaIndexNumPages =
                            fileStateNewNumPages indexEnv
                     , concurrentMetaIndexFreeTree =
                            fileStateFreeTree indexEnv
                     , concurrentMetaIndexFreshUnusedPages =
                            fileStateFreedDirtyPages indexEnv
                     }
    put (meta', env)

--------------------------------------------------------------------------------
