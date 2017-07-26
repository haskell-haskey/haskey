{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-| The module implements an page allocator with page reuse using a database of
   free pages.
 -}
module Data.BTree.Alloc.PageReuse (
  -- * Allocator
  PageReuseDb(..)

  -- * Open and create databases
, createPageReuseDb
, openPageReuseDb

  -- * Manipulation and transactions
, module Data.BTree.Alloc.Transaction
, transact
, transact_
, transactReadOnly

  -- * Storage requirements
, PageReuseMeta(..)
, PageReuseMetaStoreM(..)
) where

import Control.Applicative (Applicative(..), (<$>))
import Control.Monad.State

import Data.Binary (Binary)
import Data.List (nub)
import Data.Proxy
import Data.Typeable
import Data.Set (Set)
import qualified Data.Set as S

import GHC.Generics (Generic)

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

{-| An active page allocator that reuses fre pages. -}
data PageReuseDb hnd k v = PageReuseDb
    { pageReuseDbHandle :: hnd
    , pageReuseDbMetaId :: PageId
    , pageReuseDbMeta   :: PageReuseMeta k v
    } deriving (Show)

{-| Meta-data of the page allocator. -}
data PageReuseMeta k v = PageReuseMeta
    { pageReuseMetaRevision :: TxId
    , pageReuseMetaTree     :: Tree k v
    , pageReuseMetaFreeTree :: Tree TxId [PageId]
    , pageReuseMetaPrevious :: PageId
    } deriving (Generic, Typeable)

deriving instance (Show k, Show v) => Show (PageReuseMeta k v)

instance (Binary k, Binary v) => Binary (PageReuseMeta k v) where

{-| A class representing the storage requirements of the page allocator.

   A store supporting this page allocator should be an instance of this class.
 -}
class StoreM hnd m => PageReuseMetaStoreM hnd m where
    {-| Read a the meta-data structure from a certain page. -}
    getPageReuseMeta :: (Key k, Value v)
                     => hnd
                     -> Proxy k
                     -> Proxy v
                     -> PageId
                     -> m (PageReuseMeta k v)

    {-| Write the meta-data structure to a certain page. -}
    putPageReuseMeta :: (Key k, Value v)
                     => hnd
                     -> PageId
                     -> PageReuseMeta k v
                     -> m ()

    {-| Find the most recent meta-data structure in all pages. If
       there isn't any page that contains some meta-data, return 'Nothing'.
     -}
    openPageReuseMeta :: (Key k, Value v)
                      => hnd
                      -> Proxy k
                      -> Proxy v
                      -> m (Maybe (PageReuseMeta k v, PageId))

--------------------------------------------------------------------------------

{-| Internal monad in which page allocations can take place.

   The monad has access to an 'PageReuseMetaStoreM' back-end which manages pages
   containing the necessary page allocator meta-data

   It also has acces to a handle, which is used to properly access the page
   storage back-end.
 -}
newtype PageReuseT env m a = PageReuseT
    { fromPageReuseT :: forall hnd. PageReuseMetaStoreM hnd m =>
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

instance Functor (PageReuseT env m) where
    fmap f (PageReuseT m) = PageReuseT (fmap f m)
instance Applicative (PageReuseT env m) where
    pure a                  = PageReuseT (pure a)
    PageReuseT f <*> PageReuseT a = PageReuseT (f <*> a)
instance Monad (PageReuseT env m) where
    return          = pure
    PageReuseT m >>= f = PageReuseT (m >>= fromPageReuseT . f)

{-| Run the actions in an 'PageReuseT' monad, given a reader or writer
   environment. -}
runPageReuseT :: PageReuseMetaStoreM hnd m => PageReuseT env m a -> env hnd -> m (a, env hnd)
runPageReuseT m = runStateT (fromPageReuseT m)

{-| Evaluate the actions in an 'PageReuseT' monad, given a reader or writer
   environment. -}
evalPageReuseT :: PageReuseMetaStoreM hnd m => PageReuseT env m a -> env hnd -> m a
evalPageReuseT m env = fst <$> runPageReuseT m env

instance AllocM (PageReuseT WriterEnv m) where
    nodePageSize = PageReuseT Store.nodePageSize

    maxPageSize = PageReuseT Store.maxPageSize

    allocNode height n = getNid >>= \nid -> PageReuseT $ do
        hnd <- writerHnd <$> get
        modify' $ \env -> env { writerAllocdPages =
            S.insert (nodeIdToPageId nid) (writerAllocdPages env) }
        putNodePage hnd height nid n
        return nid
      where
        getNid :: PageReuseT WriterEnv m (NodeId height key val)
        getNid = PageReuseT (writerFreePages <$> get) >>= \case
            -- No dirty pages that are reuseable
            [] -> PageReuseT (writerReuseablePages <$> get) >>= \case
                -- No pages from a previously querying the free page database.
                [] -> nidFromFreeDb >>= \case
                    Just nid -> return nid
                    Nothing -> PageReuseT $ do
                        hnd <- writerHnd <$> get
                        pc <-  getSize hnd
                        setSize hnd (pc + 1)
                        return $! NodeId (fromPageCount pc)

                -- Use a page that was previously queried from the free page
                -- database.
                x:xs -> PageReuseT $ do
                    modify' $ \env -> env { writerReuseablePages = xs }
                    return $ pageIdToNodeId x

            -- Reuse a dirty page
            x:xs -> PageReuseT $ do
                modify' $ \env -> env { writerFreePages = xs }
                return $ pageIdToNodeId x

        -- | Try to get a free page from the free page database.
        --
        -- This function will also update the writer state.
        nidFromFreeDb :: PageReuseT WriterEnv m (Maybe (NodeId height key val))
        nidFromFreeDb = ifM (PageReuseT (not . writerReusablePagesOn <$> get)) (return Nothing) $ do
            tree    <- PageReuseT $ writerFreeTree <$> get
            oldTxId <- PageReuseT $ writerReuseablePagesTxId <$> get
            curTxId <- PageReuseT $ writerTxId <$> get

            -- Delete the previously used 'TxId' from the tree.
            -- don't reuse pages from the free pages database while editing the
            -- free pages database
            PageReuseT $ modify' $ \env -> env { writerReusablePagesOn = False }
            tree' <- maybe (return tree) (`deleteTree` tree) oldTxId
            PageReuseT $ modify' $ \env -> env { writerReusablePagesOn = True }

            PageReuseT $ modify' $ \env -> env { writerFreeTree = tree' }

            -- Lookup the oldest free pages
            lookupMinTree tree' >>= \case
                Nothing -> do
                    -- Nothing found, set state accordingly.
                    PageReuseT $ modify' $
                        \env -> env { writerReuseablePages = []
                                    , writerReuseablePagesTxId = Nothing }
                    return Nothing

                Just (txId, []) -> do
                    -- Empty list of free pages? this is an inconsistency
                    -- resolve it.
                    PageReuseT $ modify' $
                        \env -> env { writerReuseablePages = []
                                    , writerReuseablePagesTxId = Just txId }
                    nidFromFreeDb

                Just (txId, pid:pageIds) -> if
                    | txId < curTxId -> do
                        -- Found reusable pages, yeay! return first one and set
                        -- state accordingly.
                        PageReuseT $ modify' $
                            \env -> env { writerReuseablePages = pageIds
                                        , writerReuseablePagesTxId = Just txId }
                        return $ Just (pageIdToNodeId pid)
                    | otherwise -> do
                        -- Oldest transaction is not old enough, can't reuse these
                        -- pages yet.
                        PageReuseT $ modify' $
                            \env -> env { writerReuseablePages = []
                                        , writerReuseablePagesTxId = Nothing }
                        return Nothing


    writeNode nid height n = PageReuseT $ do
        hnd <- writerHnd <$> get
        putNodePage hnd height nid n
        return nid

    freeNode _ nid = PageReuseT $ modify' $ \env ->
        if S.member pid (writerAllocdPages env)
            then env { writerFreePages = pid : writerFreePages env }
            else env { writerNewlyFreedPages = pid : writerNewlyFreedPages env }
      where
        pid = nodeIdToPageId nid

instance AllocReaderM (PageReuseT WriterEnv m) where
    readNode height nid = PageReuseT $ do
        hnd <- writerHnd <$> get
        getNodePage hnd height Proxy Proxy nid

instance AllocReaderM (PageReuseT ReaderEnv m) where
    readNode height nid = PageReuseT $ do
        hnd <- readerHnd <$> get
        getNodePage hnd height Proxy Proxy nid

--------------------------------------------------------------------------------

{-| Try to open an existing database with page reuse support.

   This function tries to find the most recent valid 'PageReuseMeta' structure to
   open the database. If no such meta-data is present in the database, a new
   append-only database can be created by using 'createPageReuseDb'.
 -}
openPageReuseDb :: (Key k, Value v, PageReuseMetaStoreM hnd m)
    => hnd
    -> m (Maybe (PageReuseDb hnd k v))
openPageReuseDb hnd = do
    m <- openPageReuseMeta hnd Proxy Proxy
    case m of
        Nothing -> return Nothing
        Just (meta, metaId) ->
            return . Just $! PageReuseDb
                { pageReuseDbHandle = hnd
                , pageReuseDbMetaId = metaId
                , pageReuseDbMeta = meta
                }

{-| Create a new database with page reuse support. -}
createPageReuseDb :: forall k v hnd m. (Key k, Value v, PageReuseMetaStoreM hnd m)
    => hnd
    -> m (PageReuseDb hnd k v)
createPageReuseDb hnd = do
    let meta :: PageReuseMeta k v
        meta = PageReuseMeta
               { pageReuseMetaRevision = TxId 0
               , pageReuseMetaTree     = Tree zeroHeight Nothing
               , pageReuseMetaFreeTree = Tree zeroHeight Nothing
               , pageReuseMetaPrevious = 0
               }

    size <- getSize hnd
    setSize hnd (size + 1)
    let metaId = PageId . fromPageCount $ size
    putPageReuseMeta hnd metaId meta
    return $! PageReuseDb
        { pageReuseDbHandle = hnd
        , pageReuseDbMetaId = metaId
        , pageReuseDbMeta   = meta
        }

--------------------------------------------------------------------------------

{-| Execute a write transaction, with a result. -}
transact :: (PageReuseMetaStoreM hnd m, Key key, Value val)
         => (forall n. AllocM n => Tree key val -> n (Transaction key val a))
         -> PageReuseDb hnd key val -> m (PageReuseDb hnd key val, a)
transact act db
    | PageReuseDb
      { pageReuseDbMeta      = meta
      , pageReuseDbHandle    = hnd
      } <- db
    , PageReuseMeta
      { pageReuseMetaTree = tree
      , pageReuseMetaFreeTree = freeTree
      } <- meta
    = do
    let newRevision = pageReuseMetaRevision meta + 1
    let wEnv = WriterEnv { writerHnd = hnd
                         , writerTxId = newRevision
                         , writerNewlyFreedPages = []
                         , writerAllocdPages = S.empty
                         , writerFreePages = []
                         , writerFreeTree = freeTree
                         , writerReuseablePages = []
                         , writerReuseablePagesTxId = Nothing
                         , writerReusablePagesOn = True }
    (tx, env) <- runPageReuseT (act tree) wEnv
    case tx of
        Abort v -> return (db, v)
        Commit newTree v -> do
            -- Save the free'd pages to the free page database
            -- don't try to use pages from the free database when doing so
            freeTree' <- saveFreePages (env { writerReusablePagesOn = False })

            -- Commit
            let newMeta = PageReuseMeta
                    { pageReuseMetaRevision = newRevision
                    , pageReuseMetaTree     = newTree
                    , pageReuseMetaFreeTree = freeTree'
                    , pageReuseMetaPrevious = pageReuseDbMetaId db
                    }
            size <- getSize hnd
            setSize hnd (size + 1)
            let newMetaId = PageId . fromPageCount $ size
            putPageReuseMeta hnd newMetaId newMeta
            return (PageReuseDb
                { pageReuseDbHandle    = hnd
                , pageReuseDbMetaId    = newMetaId
                , pageReuseDbMeta      = newMeta
                }, v)
  where
    -- This function assumes that **inserting the new set of free pages** that
    -- are free'd in this specific transaction, **will eventually not free any
    -- pages itself** after sufficient iteration. Note that this is only
    -- possible because we can reuse dirty pages in the same transaction. If
    -- this is not the case, this function loops forever.
    saveFreePages :: PageReuseMetaStoreM hnd m
                  => WriterEnv hnd
                  -> m (Tree TxId [PageId])
    saveFreePages env = do
        let freeEnv = env { writerNewlyFreedPages = [] }
        (freeTree', env') <- runPageReuseT (insertFreePages env (writerFreeTree env)) freeEnv
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
transact_ :: (PageReuseMetaStoreM hnd m, Key key, Value val)
          => (forall n. AllocM n => Tree key val -> n (Transaction key val ()))
          -> PageReuseDb hnd key val -> m (PageReuseDb hnd key val)
transact_ act db = fst <$> transact act db

{-| Execute a read-only transaction. -}
transactReadOnly :: (PageReuseMetaStoreM hnd m)
                 => (forall n. AllocReaderM n => Tree key val -> n a)
                 -> PageReuseDb hnd key val -> m a
transactReadOnly act db
    | PageReuseDb
      { pageReuseDbMeta   = meta
      , pageReuseDbHandle = hnd
      } <- db
    , PageReuseMeta
      { pageReuseMetaTree = tree
      } <- meta
    = evalPageReuseT (act tree) (ReaderEnv hnd)
