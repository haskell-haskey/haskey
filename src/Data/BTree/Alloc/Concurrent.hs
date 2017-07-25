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

  -- * Open and create databases
, DatabaseHandles(..)
, createConcurrentDb
, openConcurrentDb

  -- * Manipulation and transactions
, module Data.BTree.Alloc.Transaction
, transact
, transact_
, transactReadOnly
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

--------------------------------------------------------------------------------

data CurrentMetaPage = Meta1 | Meta2

{-| An active page allocator with concurrency and page reuse. -}
data ConcurrentDb hnd k v = ConcurrentDb
    { concurrentWriterLock :: MVar ()
    , concurrentCurrentMeta :: TVar CurrentMetaPage
    , concurrentMeta1 :: ConcurrentMeta k v
    , concurrentMeta2 :: ConcurrentMeta k v
    , concurrentDbReaders :: Map TxId Integer
    }

{-| Meta-data of the page allocator. -}
data ConcurrentMeta k v = ConcurrentMeta
    { concurrentMetaRevision :: TxId
    , concurrentMetaTree     :: Tree k v
    , concurrentMetaFreeTree :: Tree TxId [PageId]
    , concurrentMetaPrevious :: PageId
    } deriving (Generic, Typeable)

deriving instance (Show k, Show v) => Show (ConcurrentMeta k v)

instance (Binary k, Binary v) => Binary (ConcurrentMeta k v) where

{-| A class representing the storage requirements of the page allocator.

   A store supporting this page allocator should be an instance of this class.
 -}
class StoreM hnd m => ConcurrentMetaStoreM hnd m where
    {-| Read a the meta-data structure from a certain page. -}
    getConcurrentMeta :: (Key k, Value v)
                      => hnd
                      -> Proxy k
                      -> Proxy v
                      -> PageId
                      -> m (ConcurrentMeta k v)

    {-| Write the meta-data structure to a certain page. -}
    putConcurrentMeta :: (Key k, Value v)
                      => hnd
                      -> PageId
                      -> ConcurrentMeta k v
                      -> m ()

    {-| Find the most recent meta-data structure in all pages. If
       there isn't any page that contains some meta-data, return 'Nothing'.
     -}
    openConcurrentMeta :: (Key k, Value v)
                       => hnd
                       -> Proxy k
                       -> Proxy v
                       -> m (Maybe (ConcurrentMeta k v, PageId))

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

instance AllocM (ConcurrentT WriterEnv m) where
    nodePageSize = ConcurrentT Store.nodePageSize

    maxPageSize = ConcurrentT Store.maxPageSize

    allocNode height n = getNid >>= \nid -> ConcurrentT $ do
        hnd <- writerHnd <$> get
        modify' $ \env -> env { writerAllocdPages =
            S.insert (nodeIdToPageId nid) (writerAllocdPages env) }
        putNodePage hnd height nid n
        return nid
      where
        getNid :: ConcurrentT WriterEnv m (NodeId height key val)
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
        nidFromFreeDb :: ConcurrentT WriterEnv m (Maybe (NodeId height key val))
        nidFromFreeDb = ifM (ConcurrentT (not . writerReusablePagesOn <$> get)) (return Nothing) $ do
            tree    <- ConcurrentT $ writerFreeTree <$> get
            oldTxId <- ConcurrentT $ writerReuseablePagesTxId <$> get
            curTxId <- ConcurrentT $ writerTxId <$> get

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

                Just (txId, pid:pageIds) -> if
                    | txId < curTxId -> do
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
data DatabaseHandles hnd = DatabaseHandles {
    databaseHandlesMain :: hnd
  , databaseHandlesMetadata1 :: hnd
  , databaseHandlesMetadata2 :: hnd
  } deriving (Show)

{-| Open the necessary database handles, and create an empty database. -}
createConcurrentDb :: (Key k, Value v, MonadIO m, ConcurrentMetaStoreM hnd m)
                   => DatabaseHandles hnd -> m (ConcurrentDb hnd k v)
createConcurrentDb DatabaseHandles{..} = do
    openHandle databaseHandlesMain
    openHandle databaseHandlesMetadata1
    openHandle databaseHandlesMetadata2
    db'     <- undefined
    readers <- liftIO Map.newIO
    meta    <- liftIO $ newTVarIO Meta1
    lock    <- liftIO $ newMVar ()
    return $! ConcurrentDb
        { concurrentWriterLock = lock
        , concurrentCurrentMeta = meta
        , concurrentMeta1 = meta0
        , concurrentMeta2 = meta0
        , concurrentDbReaders = readers
        }
  where
    meta0 = ConcurrentMeta { concurrentMetaRevision = 0
                           , concurrentMetaTree = Tree zeroHeight Nothing
                           , concurrentMetaFreeTree = Tree zeroHeight Nothing
                           , concurrentMetaPrevious = 0
                           }

{-| Open the necessary databse handles, and open an exisiting datbaase. -}
openConcurrentDb :: (Key k, Value v, MonadIO m, ConcurrentMetaStoreM hnd m)
                 => DatabaseHandles hnd -> m (Maybe (ConcurrentDb hnd k v))
openConcurrentDb = undefined

--------------------------------------------------------------------------------

{-| Execute a write transaction, with a result. -}
transact :: (MonadIO m, ConcurrentMetaStoreM hnd m, Key key, Value val)
         => (forall n. AllocM n => Tree key val -> n (Transaction key val a))
         -> ConcurrentDb hnd key val -> m (ConcurrentDb hnd key val, a)
transact act db
    | ConcurrentDb
      { concurrentWriterLock = lock
      } <- db
    = withLock lock undefined
  where
    withLock l action = do
        () <- liftIO (takeMVar l)
        v  <- action
        liftIO (putMVar l ())
        return v

{-| Execute a write transaction, without a result. -}
transact_ :: (MonadIO m, ConcurrentMetaStoreM hnd m, Key key, Value val)
          => (forall n. AllocM n => Tree key val -> n (Transaction key val ()))
          -> ConcurrentDb hnd key val -> m (ConcurrentDb hnd key val)
transact_ act db = fst <$> transact act db

{-| Execute a read-only transaction. -}
transactReadOnly :: (ConcurrentMetaStoreM hnd m)
                 => (forall n. AllocReaderM n => Tree key val -> n a)
                 -> ConcurrentDb hnd key val -> m a
transactReadOnly _ = undefined

{-| Get the current meta data. -}
currentMeta :: (MonadIO m, Key k, Value v)
            => ConcurrentDb hnd k v -> m (ConcurrentMeta k v)
currentMeta db
    | ConcurrentDb { concurrentCurrentMeta = v } <- db
    = liftIO (atomically $ readTVar v) >>= \case
        Meta1 -> return $! concurrentMeta1 db
        Meta2 -> return $! concurrentMeta2 db

setCurrentMeta :: (MonadIO m, Key k, Value v)
               => ConcurrentDb hnd k v -> ConcurrentMeta k v -> m (ConcurrentDb hnd k v)
setCurrentMeta db new
    | ConcurrentDb { concurrentCurrentMeta = v } <- db
    = liftIO (atomically $ readTVar v) >>= \case
        Meta1 -> do liftIO . atomically $ writeTVar v Meta2
                    return $! db { concurrentMeta2 = new }
        Meta2 -> do liftIO . atomically $ writeTVar v Meta1
                    return $! db { concurrentMeta1 = new }
