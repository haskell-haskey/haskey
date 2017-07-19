{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-| The module implements an append-only page allocator.

   All written data will be written to newly created pages, and their is
   absolutetely no page reuse.
 -}
module Data.BTree.Alloc.Append (
  -- * Allocator
  AppendDb(..)

  -- * Open and create databases
, createAppendDb
, openAppendDb

  -- * Manipulation and transactions
, Transaction
, transact
, transact_
, transactReadOnly
, commit
, commit_
, abort
, abort_

  -- * Storage requirements
, AppendMeta(..)
, AppendMetaStoreM(..)
) where

import Control.Applicative (Applicative(..), (<$>))
import Control.Monad.State

import Data.Binary (Binary)
import Data.Proxy
import Data.Typeable
import Data.Set (Set)
import qualified Data.Set as S

import GHC.Generics (Generic)

import Data.BTree.Alloc.Class
import Data.BTree.Impure.Structures
import Data.BTree.Primitives
import Data.BTree.Store.Class
import qualified Data.BTree.Store.Class as Store

--------------------------------------------------------------------------------

{-| An active append-only page allocator. -}
data AppendDb hnd k v = AppendDb
    { appendDbHandle :: hnd
    , appendDbMetaId :: PageId
    , appendDbMeta   :: AppendMeta k v
    , appendDbFreePages :: [PageId]
    } deriving (Show)

{-| Meta-data of an append-only page allocator. -}
data AppendMeta k v = AppendMeta
    { appendMetaRevision :: TxId
    , appendMetaTree     :: Tree k v
    , appendMetaPrevious :: PageId
    } deriving (Generic, Typeable)

deriving instance (Show k, Show v) => Show (AppendMeta k v)

instance (Binary k, Binary v) => Binary (AppendMeta k v) where

{-| A class representing the storage requirements of the append-only page
   allocator.

   A store supporting the append-only page allocator should be an instance of
   this class.
 -}
class StoreM hnd m => AppendMetaStoreM hnd m where
    {-| Read a the append-only meta-data structure from a certain page. -}
    getAppendMeta :: (Key k, Value v)
                  => hnd
                  -> Proxy k
                  -> Proxy v
                  -> PageId
                  -> m (AppendMeta k v)

    {-| Write the append-only meta-data structure to a certain page. -}
    putAppendMeta :: (Key k, Value v) => hnd -> PageId -> AppendMeta k v -> m ()

    {-| Find the most recent append-only meta-data structure in all pages. If
       there isn't any page that contains some meta-data, return 'Nothing'.
     -}
    openAppendMeta :: (Key k, Value v)
                 => hnd
                 -> Proxy k
                 -> Proxy v
                 -> m (Maybe (AppendMeta k v, PageId))

--------------------------------------------------------------------------------

{-| Internal monad in which append-only page allocations can take place.

   The monad has access to an 'AppendMetaStoreM' back-end which manages pages
   containing the necessary append-only page allocator meta-data

   It also has acces to a handle, which is used to properly access the page
   storage back-end.
 -}
newtype AppendT env m a = AppendT
    { fromAppendT :: forall hnd. AppendMetaStoreM hnd m =>
        StateT (env hnd) m a
    }

newtype ReaderEnv hnd = ReaderEnv { readerHnd :: hnd }

data WriterEnv hnd = WriterEnv
    { writerHnd :: hnd
    , writerTxId :: TxId
    , writerNewlyFreedPages :: [PageId] -- ^ Pages free'd in this transaction,
                                        -- not ready for reuse until the
                                        -- transaction is commited.
    , writerAllocdPages :: Set PageId -- ^ Pages allocated in this transcation.
                                      -- These pages can be reused in the same
                                      -- transaction if free'd later.
    , writerFreePages :: [PageId] -- ^ Pages free for immediate reuse.
    }

instance Functor (AppendT env m) where
    fmap f (AppendT m) = AppendT (fmap f m)
instance Applicative (AppendT env m) where
    pure a                  = AppendT (pure a)
    AppendT f <*> AppendT a = AppendT (f <*> a)
instance Monad (AppendT env m) where
    return          = pure
    AppendT m >>= f = AppendT (m >>= fromAppendT . f)

{-| Run the actions in an 'AppendT' monad, given a reader or writer
   environment. -}
runAppendT :: AppendMetaStoreM hnd m => AppendT env m a -> env hnd -> m (a, env hnd)
runAppendT m = runStateT (fromAppendT m)

{-| Evaluate the actions in an 'AppendT' monad, given a reader or writer
   environment. -}
evalAppendT :: AppendMetaStoreM hnd m => AppendT env m a -> env hnd -> m a
evalAppendT m env = fst <$> runAppendT m env

instance AllocWriterM (AppendT WriterEnv m) where
    nodePageSize = AppendT Store.nodePageSize

    maxPageSize = AppendT Store.maxPageSize

    allocNode height n = AppendT $ do
        hnd <- writerHnd <$> get
        nid <- getNid
        modify $ \env -> env { writerAllocdPages =
            S.insert (nodeIdToPageId nid) (writerAllocdPages env) }
        putNodePage hnd height nid n
        return nid
      where
        getNid :: AppendMetaStoreM hnd m
               => StateT (WriterEnv hnd) m (NodeId height key val)
        getNid = (writerFreePages <$> get) >>= \case
            [] -> do
                hnd <- writerHnd <$> get
                pc <-  getSize hnd
                setSize hnd (pc + 1)
                return $! NodeId (fromPageCount pc)
            x:xs -> do
                modify $ \env -> env { writerFreePages = xs }
                return $ pageIdToNodeId x


    writeNode nid height n = AppendT $ do
        hnd <- writerHnd <$> get
        putNodePage hnd height nid n
        return nid

    freeNode _ nid = AppendT $ modify $ \env ->
        if S.member pid (writerAllocdPages env)
            then env { writerFreePages = pid : writerFreePages env }
            else env { writerNewlyFreedPages = pid : writerNewlyFreedPages env }
      where
        pid = nodeIdToPageId nid

    currentTxId = AppendT $ writerTxId <$> get

instance AllocReaderM (AppendT WriterEnv m) where
    readNode height nid = AppendT $ do
        hnd <- writerHnd <$> get
        getNodePage hnd height Proxy Proxy nid

instance AllocReaderM (AppendT ReaderEnv m) where
    readNode height nid = AppendT $ do
        hnd <- readerHnd <$> get
        getNodePage hnd height Proxy Proxy nid

--------------------------------------------------------------------------------

{-| Try to open an existing append-only database.

   This function tries to find the most recent valid 'AppendMeta' structure to
   open the database. If no such meta-data is present in the database, a new
   append-only database can be created by using 'createAppendDb'.
 -}
openAppendDb :: (Key k, Value v, AppendMetaStoreM hnd m)
    => hnd
    -> m (Maybe (AppendDb hnd k v))
openAppendDb hnd = do
    m <- openAppendMeta hnd Proxy Proxy
    case m of
        Nothing -> return Nothing
        Just (meta, metaId) ->
            return . Just $! AppendDb
                { appendDbHandle = hnd
                , appendDbMetaId = metaId
                , appendDbMeta = meta
                , appendDbFreePages = []
                }

{-| Create a new append-only database. -}
createAppendDb :: forall k v hnd m. (Key k, Value v, AppendMetaStoreM hnd m)
    => hnd
    -> m (AppendDb hnd k v)
createAppendDb hnd = do
    let meta :: AppendMeta k v
        meta = AppendMeta
               { appendMetaRevision = TxId 0
               , appendMetaTree     = Tree zeroHeight Nothing
               , appendMetaPrevious = 0
               }

    size <- getSize hnd
    setSize hnd (size + 1)
    let metaId = PageId . fromPageCount $ size
    putAppendMeta hnd metaId meta
    return $! AppendDb
        { appendDbHandle = hnd
        , appendDbMetaId = metaId
        , appendDbMeta   = meta
        , appendDbFreePages = []
        }

--------------------------------------------------------------------------------

{-| A committed or aborted transaction, with a return value of type @a@. -}
data Transaction key val a =
      Commit (Tree key val) a
    | Abort a

{-| Commit the new tree and return a computed value. -}
commit :: AllocM n => a -> Tree key val -> n (Transaction key val a)
commit v t = return $ Commit t v

{-| Commit the new tree, without return a computed value. -}
commit_ :: AllocM n => Tree key val -> n (Transaction key val ())
commit_ = commit ()

{-| Abort the transaction and return a computed value. -}
abort :: AllocM n => a -> n (Transaction key val a)
abort = return . Abort

{-| Abort the transaction, without returning a computed value. -}
abort_ :: AllocM n => n (Transaction key val ())
abort_ = return $ Abort ()

{-| Execute a write transaction, with a result. -}
transact :: (AppendMetaStoreM hnd m, Key key, Value val)
         => (forall n. AllocM n => Tree key val -> n (Transaction key val a))
         -> AppendDb hnd key val -> m (AppendDb hnd key val, a)
transact act db
    | AppendDb
      { appendDbMeta      = meta
      , appendDbHandle    = hnd
      , appendDbFreePages = freePages
      } <- db
    , AppendMeta
      { appendMetaTree = tree
      } <- meta
    = do
    let newRevision = appendMetaRevision meta + 1
    let wEnv = WriterEnv { writerHnd = hnd
                         , writerTxId = newRevision
                         , writerNewlyFreedPages = []
                         , writerAllocdPages = S.empty
                         , writerFreePages = freePages }
    (tx, env) <- runAppendT (act tree) wEnv
    case tx of
        Abort v -> return (db, v)
        Commit newTree v -> do
            let newMeta = AppendMeta
                    { appendMetaRevision = newRevision
                    , appendMetaTree     = newTree
                    , appendMetaPrevious = appendDbMetaId db
                    }
            size <- getSize hnd
            setSize hnd (size + 1)
            let newMetaId = PageId . fromPageCount $ size
            putAppendMeta hnd newMetaId newMeta
            return (AppendDb
                { appendDbHandle    = hnd
                , appendDbMetaId    = newMetaId
                , appendDbMeta      = newMeta
                , appendDbFreePages =
                    writerNewlyFreedPages env ++ writerFreePages env
                }, v)

{-| Execute a write transaction, without a result. -}
transact_ :: (AppendMetaStoreM hnd m, Key key, Value val)
          => (forall n. AllocM n => Tree key val -> n (Transaction key val ()))
          -> AppendDb hnd key val -> m (AppendDb hnd key val)
transact_ act db = fst <$> transact act db

{-| Execute a read-only transaction. -}
transactReadOnly :: (AppendMetaStoreM hnd m)
                 => (forall n. AllocReaderM n => Tree key val -> n a)
                 -> AppendDb hnd key val -> m a
transactReadOnly act db
    | AppendDb
      { appendDbMeta   = meta
      , appendDbHandle = hnd
      } <- db
    , AppendMeta
      { appendMetaTree = tree
      } <- meta
    = evalAppendT (act tree) (ReaderEnv hnd)
