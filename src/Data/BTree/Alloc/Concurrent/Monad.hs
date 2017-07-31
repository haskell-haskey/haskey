{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE RankNTypes #-}
-- | This module implements the 'ConcurrentT' monad.
--
-- The 'ConcurrentT' monad is used to implement a page allocator with
-- concurrent readers and serialized writers.
module Data.BTree.Alloc.Concurrent.Monad where

import Control.Concurrent.STM
import Control.Monad.State

import Data.Proxy (Proxy(..))
import Data.Set (Set)
import qualified Data.Set as S

import STMContainers.Map (Map)

import Data.BTree.Alloc.Class
import Data.BTree.Alloc.Concurrent.Meta
import Data.BTree.Primitives
import Data.BTree.Impure
import Data.BTree.Store
import Data.BTree.Utils.Monad (ifM)
import qualified Data.BTree.Store.Class as Store
import qualified Data.BTree.Utils.STM.Map as Map

-- | Monad in which page allocations can take place.
--
-- The monad has access to a 'ConcurrentMetaStoreM' back-end which manages can
-- store and retreive the corresponding metadata.
newtype ConcurrentT env m a = ConcurrentT
  { fromConcurrentT :: forall hnd. ConcurrentMetaStoreM  hnd m =>
      StateT (env hnd) m a
  }

instance Functor (ConcurrentT env m) where
    fmap f (ConcurrentT m) = ConcurrentT (fmap f m)

instance Applicative (ConcurrentT env m) where
    pure a = ConcurrentT (pure a)
    ConcurrentT f <*> ConcurrentT a = ConcurrentT (f <*> a)

instance Monad (ConcurrentT env m) where
    return = pure
    ConcurrentT m >>= f = ConcurrentT (m >>= fromConcurrentT . f)

-- | Run the actions in an 'ConcurrentT' monad, given a reader or writer
-- environment. -}
runConcurrentT :: ConcurrentMetaStoreM hnd m => ConcurrentT env m a -> env hnd -> m (a, env hnd)
runConcurrentT m = runStateT (fromConcurrentT m)

-- | Evaluate the actions in an 'ConcurrentT' monad, given a reader or writer
-- environment. -}
evalConcurrentT :: ConcurrentMetaStoreM hnd m => ConcurrentT env m a -> env hnd -> m a
evalConcurrentT m env = fst <$> runConcurrentT m env

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
