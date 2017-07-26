{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StandaloneDeriving #-}
{-| The module implements an page allocator with page reuse and support for
   multiple readers and serialized writers.
 -}
module Data.BTree.Alloc.Concurrent (
  -- * Allocator
  ConcurrentDb(..)

  -- * Open, close and create databases
, ConcurrentHandles(..)
, openConcurrentHandles
, closeConcurrentHandles
, createConcurrentDb
, openConcurrentDb

  -- * Manipulation and transactions
, module Data.BTree.Alloc.Transaction
, transact
, transact_
, transactReadOnly

  -- * Storage requirements
, ConcurrentMeta(..)
, ConcurrentMetaStoreM(..)
) where

import Control.Concurrent.MVar
import Control.Concurrent.STM

import Control.Applicative (Applicative(..), (<$>))
import Control.Monad.IO.Class
import Control.Monad.State

import Data.Binary (Binary)
import Data.List (nub)
import Data.Proxy
import Data.Typeable
import Data.Set (Set)
import qualified Data.Set as S

import GHC.Generics (Generic)

import STMContainers.Map (Map)
import qualified STMContainers.Map as Map

import Data.BTree.Alloc.Class
import Data.BTree.Alloc.Transaction
import Data.BTree.Impure.Delete
import Data.BTree.Impure.Insert
import Data.BTree.Impure.Lookup
import Data.BTree.Impure.Structures
import Data.BTree.Primitives
import Data.BTree.Store.Class
import Data.BTree.Utils.Monad (ifM)
import qualified Data.BTree.Store.Class as Store
import qualified Data.BTree.Utils.STM.Map as Map

--------------------------------------------------------------------------------

data CurrentMetaPage = Meta1 | Meta2

{-| An active page allocator with concurrency and page reuse.

   This can be shared among threads. -}
data ConcurrentDb hnd k v = ConcurrentDb
    { concurrentDbHandles :: ConcurrentHandles hnd
    , concurrentDbWriterLock :: MVar ()
    , concurrentDbCurrentMeta :: TVar CurrentMetaPage
    , concurrentDbMeta1 :: TVar (ConcurrentMeta k v)
    , concurrentDbMeta2 :: TVar (ConcurrentMeta k v)
    , concurrentDbReaders :: Map TxId Integer
    }

{-| Meta-data of the page allocator. -}
data ConcurrentMeta k v = ConcurrentMeta
    { concurrentMetaRevision :: TxId
    , concurrentMetaTree     :: Tree k v
    , concurrentMetaFreeTree :: Tree TxId [PageId]
    } deriving (Generic, Typeable)

deriving instance (Show k, Show v) => Show (ConcurrentMeta k v)

instance (Binary k, Binary v) => Binary (ConcurrentMeta k v) where

{-| A class representing the storage requirements of the page allocator.

   A store supporting this page allocator should be an instance of this class.
 -}
class StoreM hnd m => ConcurrentMetaStoreM hnd m where
    {-| Write the meta-data structure to a certain page. -}
    putConcurrentMeta :: (Key k, Value v)
                      => hnd
                      -> PageId
                      -> ConcurrentMeta k v
                      -> m ()

    {-| Find the most recent meta-data structure. If there isn't any page that
       contains some meta-data, return 'Nothing'.
     -}
    openConcurrentMeta :: (Key k, Value v)
                       => ConcurrentHandles hnd
                       -> Proxy k
                       -> Proxy v
                       -> m (Maybe (ConcurrentMeta k v))

--------------------------------------------------------------------------------

{-| Internal monad in which page allocations can take place.

   The monad has access to an 'ConcurrentMetaStoreM' back-end which manages pages
   containing the necessary page allocator meta-data

   It also has acces to a handle, which is used to properly access the page
   storage back-end.
 -}
newtype ConcurrentT env m a = ConcurrentT
    { fromConcurrentT :: forall hnd. ConcurrentMetaStoreM hnd m =>
        StateT (env hnd) m a
    }

newtype ReaderEnv hnd = ReaderEnv { readerHnd :: hnd }

data WriterEnv hnd = WriterEnv
    { writerHnd :: !hnd
    , writerTxId :: !TxId
    , writerReaders :: Map TxId Integer
    , writerNewlyFreedPages :: ![PageId] -- ^ Pages free'd in this transaction,
                                         -- not ready for reuse until the
                                         -- transaction is commited.
    , writerAllocdPages :: !(Set PageId) -- ^ Pages allocated in this transcation.
                                         -- These pages can be reused in the same
                                         -- transaction if free'd later.
    , writerFreePages :: ![PageId] -- ^ Pages free for immediate reuse.
    , writerFreeTree :: !(Tree TxId [PageId]) -- ^ The root of the free tree,
                                            -- might change during a
                                            -- transaction.
    , writerReuseablePages :: ![PageId] -- ^ Pages queried from the free pages
                                        -- database and ready for immediate reuse.
    , writerReuseablePagesTxId :: !(Maybe TxId) -- ^ The 'TxId' of the pages in
                                                -- 'writerReuseablePages', or 'Nothing'
                                                -- if no pages were queried yet from
                                                -- the free database.
    , writerReusablePagesOn :: !Bool -- ^ Used to turn of querying the free page
                                     -- database for free pages.
    }

instance Functor (ConcurrentT env m) where
    fmap f (ConcurrentT m) = ConcurrentT (fmap f m)
instance Applicative (ConcurrentT env m) where
    pure a                  = ConcurrentT (pure a)
    ConcurrentT f <*> ConcurrentT a = ConcurrentT (f <*> a)
instance Monad (ConcurrentT env m) where
    return          = pure
    ConcurrentT m >>= f = ConcurrentT (m >>= fromConcurrentT . f)

{-| Run the actions in an 'ConcurrentT' monad, given a reader or writer
   environment. -}
runConcurrentT :: ConcurrentMetaStoreM hnd m => ConcurrentT env m a -> env hnd -> m (a, env hnd)
runConcurrentT m = runStateT (fromConcurrentT m)

{-| Evaluate the actions in an 'ConcurrentT' monad, given a reader or writer
   environment. -}
evalConcurrentT :: ConcurrentMetaStoreM hnd m => ConcurrentT env m a -> env hnd -> m a
evalConcurrentT m env = fst <$> runConcurrentT m env

instance MonadIO m => AllocM (ConcurrentT WriterEnv m) where
    nodePageSize = ConcurrentT Store.nodePageSize

    maxPageSize = ConcurrentT Store.maxPageSize

    allocNode height n = getNid >>= \nid -> ConcurrentT $ do
        hnd <- writerHnd <$> get
        modify' $ \env -> env { writerAllocdPages =
            S.insert (nodeIdToPageId nid) (writerAllocdPages env) }
        putNodePage hnd height nid n
        return nid
      where
        getNid :: MonadIO m => ConcurrentT WriterEnv m (NodeId height key val)
        getNid = ConcurrentT (writerFreePages <$> get) >>= \case
            -- No dirty pages that are reuseable
            [] -> ConcurrentT (writerReuseablePages <$> get) >>= \case
                -- No pages from a previously querying the free page database.
                [] -> nidFromFreeDb >>= \case
                    Just nid -> return nid
                    Nothing -> ConcurrentT $ do
                        hnd <- writerHnd <$> get
                        pc <-  getSize hnd
                        setSize hnd (pc + 1)
                        return $! NodeId (fromPageCount pc)

                -- Use a page that was previously queried from the free page
                -- database.
                x:xs -> ConcurrentT $ do
                    modify' $ \env -> env { writerReuseablePages = xs }
                    return $ pageIdToNodeId x

            -- Reuse a dirty page
            x:xs -> ConcurrentT $ do
                modify' $ \env -> env { writerFreePages = xs }
                return $ pageIdToNodeId x

        -- | Try to get a free page from the free page database.
        --
        -- This function will also update the writer state.
        nidFromFreeDb :: MonadIO m => ConcurrentT WriterEnv m (Maybe (NodeId height key val))
        nidFromFreeDb = ifM (ConcurrentT (not . writerReusablePagesOn <$> get)) (return Nothing) $ do
            tree    <- ConcurrentT $ writerFreeTree <$> get
            oldTxId <- ConcurrentT $ writerReuseablePagesTxId <$> get

            -- Delete the previously used 'TxId' from the tree.
            -- don't reuse pages from the free pages database while editing the
            -- free pages database
            ConcurrentT $ modify' $ \env -> env { writerReusablePagesOn = False }
            tree' <- maybe (return tree) (`deleteTree` tree) oldTxId
            ConcurrentT $ modify' $ \env -> env { writerReusablePagesOn = True }

            ConcurrentT $ modify' $ \env -> env { writerFreeTree = tree' }

            -- Lookup the oldest free pages
            lookupMinTree tree' >>= \case
                Nothing -> do
                    -- Nothing found, set state accordingly.
                    ConcurrentT $ modify' $
                        \env -> env { writerReuseablePages = []
                                    , writerReuseablePagesTxId = Nothing }
                    return Nothing

                Just (txId, []) -> do
                    -- Empty list of free pages? this is an inconsistency
                    -- resolve it.
                    ConcurrentT $ modify' $
                        \env -> env { writerReuseablePages = []
                                    , writerReuseablePagesTxId = Just txId }
                    nidFromFreeDb

                Just (txId, pid:pageIds) -> do
                    readers <- ConcurrentT $ writerReaders <$> get
                    oldest  <- ConcurrentT $ liftIO . atomically $ Map.lookupMinKey readers
                    if | maybe True (txId <) oldest -> do
                        -- Found reusable pages, yeay! return first one and set
                        -- state accordingly.
                        ConcurrentT $ modify' $
                            \env -> env { writerReuseablePages = pageIds
                                        , writerReuseablePagesTxId = Just txId }
                        return $ Just (pageIdToNodeId pid)
                       | otherwise -> do
                        -- Oldest transaction is not old enough, can't reuse these
                        -- pages yet.
                        ConcurrentT $ modify' $
                            \env -> env { writerReuseablePages = []
                                        , writerReuseablePagesTxId = Nothing }
                        return Nothing


    writeNode nid height n = ConcurrentT $ do
        hnd <- writerHnd <$> get
        putNodePage hnd height nid n
        return nid

    freeNode _ nid = ConcurrentT $ modify' $ \env ->
        if S.member pid (writerAllocdPages env)
            then env { writerFreePages = pid : writerFreePages env }
            else env { writerNewlyFreedPages = pid : writerNewlyFreedPages env }
      where
        pid = nodeIdToPageId nid

instance AllocReaderM (ConcurrentT WriterEnv m) where
    readNode height nid = ConcurrentT $ do
        hnd <- writerHnd <$> get
        getNodePage hnd height Proxy Proxy nid

instance AllocReaderM (ConcurrentT ReaderEnv m) where
    readNode height nid = ConcurrentT $ do
        hnd <- readerHnd <$> get
        getNodePage hnd height Proxy Proxy nid

--------------------------------------------------------------------------------

{-| All necessary database handles. -}
data ConcurrentHandles hnd = ConcurrentHandles {
    concurrentHandlesMain :: hnd
  , concurrentHandlesMetadata1 :: hnd
  , concurrentHandlesMetadata2 :: hnd
  } deriving (Show)

{-| Open all concurrent handles. -}
openConcurrentHandles :: ConcurrentMetaStoreM hnd m
                      => ConcurrentHandles hnd -> m ()
openConcurrentHandles ConcurrentHandles{..} = do
    openHandle concurrentHandlesMain
    openHandle concurrentHandlesMetadata1
    openHandle concurrentHandlesMetadata2

{-| Open a new concurrent database, with the given handles.

  The handles should already have been opened using 'openConcurrentHandles'.
 -}
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

{-| Open the an existing database, with the given handles.

  The handles should already have been opened using 'openConcurrentHandles'.
 -}
openConcurrentDb :: (Key k, Value v, MonadIO m, ConcurrentMetaStoreM hnd m)
                 => ConcurrentHandles hnd -> m (Maybe (ConcurrentDb hnd k v))
openConcurrentDb hnds = do
    m <- openConcurrentMeta hnds Proxy Proxy
    case m of
        Nothing -> return Nothing
        Just meta0 -> Just <$> newConcurrentDb hnds meta0

{-| Close the handles of the database. -}
closeConcurrentHandles :: (MonadIO m, ConcurrentMetaStoreM hnd m)
                       => ConcurrentHandles hnd
                       -> m ()
closeConcurrentHandles ConcurrentHandles{..} = do
    closeHandle concurrentHandlesMain
    closeHandle concurrentHandlesMetadata1
    closeHandle concurrentHandlesMetadata2

{-| Create a new concurrent database with handles and metadata provided. -}
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

--------------------------------------------------------------------------------

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
    meta <- getCurrentMeta db
    let newRevision = concurrentMetaRevision meta + 1
    let wEnv = WriterEnv { writerHnd = hnd
                         , writerTxId = newRevision
                         , writerReaders = readers
                         , writerNewlyFreedPages = []
                         , writerAllocdPages = S.empty
                         , writerFreePages = []
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
            freeTree' <- saveFreePages (env { writerReusablePagesOn = False })

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

    -- This function assumes that **inserting the new set of free pages** that
    -- are free'd in this specific transaction, **will eventually not free any
    -- pages itself** after sufficient iteration. Note that this is only
    -- possible because we can reuse dirty pages in the same transaction. If
    -- this is not the case, this function loops forever.
    saveFreePages :: (MonadIO m, ConcurrentMetaStoreM hnd m)
                  => WriterEnv hnd
                  -> m (Tree TxId [PageId])
    saveFreePages env = do
        let freeEnv = env { writerNewlyFreedPages = [] }
        (freeTree', env') <- runConcurrentT (insertFreePages env (writerFreeTree env)) freeEnv
        case writerNewlyFreedPages env' of
            [] -> return freeTree'
            xs  -> let env'' = env' { writerNewlyFreedPages =
                                        nub (xs ++ writerNewlyFreedPages env')
                                    , writerFreeTree = freeTree' } in
                   saveFreePages env'' -- Register newly free'd pages

    insertFreePages :: AllocM n
                    => WriterEnv hnd
                    -> Tree TxId [PageId]
                    -> n (Tree TxId [PageId])
    insertFreePages env t
        -- No newly free'd pages in this tx, and no pages reused from free page db
        | []      <- writerNewlyFreedPages env
        , Nothing <- writerReuseablePagesTxId env
        = return t

        -- Only pages reused from free page db, no newly free'd pages in this tx
        | []      <- writerNewlyFreedPages env
        , Just k' <- writerReuseablePagesTxId env
        , v'      <- writerReuseablePages env
        = if null v' then deleteTree k' t else insertTree k' v' t

        -- Only newly free'd pages in this tx, and no pages reused from free page db
        | v       <- writerNewlyFreedPages env
        , k       <- writerTxId env
        , Nothing <- writerReuseablePagesTxId env
        = insertTree k v t

        -- Pages reused from free page db, and newly free'd pages in this tx
        | v       <- writerNewlyFreedPages env
        , k       <- writerTxId env
        , v'      <- writerReuseablePages env
        , Just k' <- writerReuseablePagesTxId env
        = (if null v' then deleteTree k' t else insertTree k' v' t)
        >>= insertTree k v

        | otherwise = error "imposible"

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
    meta <- getCurrentMeta db
    liftIO . atomically $ Map.alter (concurrentMetaRevision meta) addOne readers
    v <- evalConcurrentT (act $ concurrentMetaTree meta) (ReaderEnv hnd)
    liftIO . atomically $ Map.alter (concurrentMetaRevision meta) subOne readers
    return v
  where
    addOne Nothing = Just 1
    addOne (Just x) = Just $! x + 1
    subOne Nothing = Nothing
    subOne (Just 0) = Nothing
    subOne (Just x) = Just $! x - 1

{-| Get the current meta data. -}
getCurrentMeta :: (MonadIO m, Key k, Value v)
               => ConcurrentDb hnd k v -> m (ConcurrentMeta k v)
getCurrentMeta db
    | ConcurrentDb { concurrentDbCurrentMeta = v } <- db
    = liftIO . atomically $ readTVar v >>= \case
        Meta1 -> readTVar $ concurrentDbMeta1 db
        Meta2 -> readTVar $ concurrentDbMeta2 db

{-| Write the new metadata, and switch the pointer to the current one. -}
setCurrentMeta :: (MonadIO m, ConcurrentMetaStoreM hnd m, Key k, Value v)
               => ConcurrentMeta k v -> ConcurrentDb hnd k v -> m ()
setCurrentMeta new db
    | ConcurrentDb
      { concurrentDbCurrentMeta = v
      , concurrentDbHandles = hnds
      } <- db
    = liftIO (atomically $ readTVar v) >>= \case
        Meta1 -> do
            putConcurrentMeta (concurrentHandlesMetadata2 hnds) 0 new
            liftIO . atomically $ do
                writeTVar v Meta2
                writeTVar (concurrentDbMeta2 db) new
        Meta2 -> do
            putConcurrentMeta (concurrentHandlesMetadata1 hnds) 0 new
            liftIO . atomically $ do
                writeTVar v Meta1
                writeTVar (concurrentDbMeta1 db) new
