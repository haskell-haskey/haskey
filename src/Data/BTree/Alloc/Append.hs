{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE StandaloneDeriving        #-}

module Data.BTree.Alloc.Append where

import           Data.BTree.Delete
import           Data.BTree.Insert
import           Data.BTree.Alloc.Class
import           Data.BTree.Primitives
import           Data.BTree.Store.Class
import qualified Data.BTree.Store.Class as Store

import           Control.Applicative (Applicative(..), (<$>))
import           Control.Monad.Reader.Class
import           Control.Monad.Trans.Reader (ReaderT, runReaderT)
import qualified Data.ByteString.Lazy as BL
import           Data.Binary (Binary, encode)
import           Data.Typeable

import           GHC.Generics (Generic)

--------------------------------------------------------------------------------

class StoreM hnd m => AppendMetaStoreM hnd m where
    getAppendMeta :: (Key k, Value v) => hnd -> PageId -> m (AppendMeta k v)
    putAppendMeta :: (Key k, Value v) => hnd -> PageId -> AppendMeta k v -> m ()

data AppendMeta k v = AppendMeta
    { appendMetaRevision :: TxId
    , appendMetaTree     :: Tree k v
    , appendMetaPrevious :: PageId
    } deriving (Generic, Typeable)

deriving instance (Show k, Show v) => Show (AppendMeta k v)

instance (Binary k, Binary v) => Binary (AppendMeta k v) where

--------------------------------------------------------------------------------

newtype AppendT m a = AppendT
    { fromAppendT :: forall hnd. AppendMetaStoreM hnd m =>
        ReaderT hnd m a
    }

instance Functor (AppendT m) where
    fmap f (AppendT m) = AppendT (fmap f m)
instance Applicative (AppendT m) where
    pure a                  = AppendT (pure a)
    AppendT f <*> AppendT a = AppendT (f <*> a)
instance Monad (AppendT m) where
    return          = pure
    AppendT m >>= f = AppendT (m >>= fromAppendT . f)

runAppendT :: AppendMetaStoreM hnd m => AppendT m a -> hnd -> m a
runAppendT m = runReaderT (fromAppendT m)

instance AllocM (AppendT m) where
    nodeSize = return $ \n -> fromIntegral . BL.length $ case n of
        Idx{}  -> encode n
        Leaf{} -> encode n
    maxNodeSize        = AppendT Store.maxNodeSize
    allocNode height n = AppendT $ do
        hnd <- ask
        pc <- getSize hnd
        setSize hnd (pc+1)
        let nid = NodeId (fromPageCount pc)
        putNodePage hnd height nid n
        return nid
    readNode height nid = AppendT $ do
        hnd <- ask
        getNodePage hnd height nid
    freeNode _height _nid = return ()

--------------------------------------------------------------------------------

data AppendDb hnd k v = AppendDb
    { appendDbHandle :: hnd
    , appendDbMetaId :: PageId
    , appendDbMeta   :: AppendMeta k v
    } deriving (Show)

createAppendDb :: forall k v hnd m. (Key k, Value v, AppendMetaStoreM hnd m)
    => hnd
    -> m (AppendDb hnd k v)
createAppendDb hnd = do
    let metaId :: PageId
        metaId = 0
        meta :: AppendMeta k v
        meta = AppendMeta
               { appendMetaRevision = TxId 0
               , appendMetaTree     = Tree zeroHeight Nothing
               , appendMetaPrevious = metaId
               }

    setSize hnd 1
    putAppendMeta hnd metaId meta
    return $! AppendDb
        { appendDbHandle = hnd
        , appendDbMetaId = metaId
        , appendDbMeta   = meta
        }

--------------------------------------------------------------------------------

readTransact :: (AppendMetaStoreM hnd m)
             => (forall n. AllocM n => Tree key val -> n a)
             -> AppendDb hnd key val -> m a
readTransact act db
    | AppendDb
      { appendDbMeta   = meta
      , appendDbHandle = hnd
      } <- db
    , AppendMeta
      { appendMetaTree = tree
      } <- meta
    = runAppendT (act tree) hnd

transact :: (AppendMetaStoreM hnd m, Key key, Value val)
         => (forall n. AllocM n => Tree key val -> n (Tree key val))
         -> AppendDb hnd key val -> m (AppendDb hnd key val)
transact act db
    | AppendDb
      { appendDbMeta   = meta
      , appendDbHandle = hnd
      } <- db
    , AppendMeta
      { appendMetaTree = tree
      } <- meta
    = do
          newTree <- runAppendT (act tree) hnd
          let newMeta = AppendMeta
                  { appendMetaRevision = appendMetaRevision meta + 1
                  , appendMetaTree     = newTree
                  , appendMetaPrevious = appendDbMetaId db
                  }
          newMetaId <- PageId . fromPageCount <$> getSize hnd
          putAppendMeta hnd newMetaId newMeta
          return AppendDb
              { appendDbHandle = hnd
              , appendDbMetaId = newMetaId
              , appendDbMeta   = newMeta
              }

--------------------------------------------------------------------------------

insert :: (AppendMetaStoreM hnd m, Key k, Value v)
    => k
    -> v
    -> AppendDb hnd k v -> m (AppendDb hnd k v)
insert k v = transact (insertTree k v)

delete :: (AppendMetaStoreM hnd m, Key k, Value v)
    => k
    -> AppendDb hnd k v -> m (AppendDb hnd k v)
delete k = transact (deleteTree k)

--------------------------------------------------------------------------------
