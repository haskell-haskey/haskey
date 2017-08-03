{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}
-- | This module implements data structures and function related to the
-- metadata of the concurrent page allocator.
module Data.BTree.Alloc.Concurrent.Meta where

import Data.Binary (Binary)
import Data.Proxy (Proxy)

import GHC.Generics (Generic)

import Data.BTree.Impure.Structures
import Data.BTree.Primitives
import Data.BTree.Store

-- | Data type used to point to the most recent version of the meta data.
data CurrentMetaPage = Meta1 | Meta2

-- | Meta data of the page allocator.
data ConcurrentMeta k v = ConcurrentMeta {
    concurrentMetaRevision :: TxId
  , concurrentMetaTree :: Tree k v
  , concurrentMetaFreeTree :: Tree TxId [PageId]
  } deriving (Generic)

deriving instance (Show k, Show v) => Show (ConcurrentMeta k v)

instance (Binary k, Binary v) => Binary (ConcurrentMeta k v) where

-- | A class representing the storage requirements of the page allocator.
--
-- A store supporting the page allocator should be an instance of this class.
class StoreM hnd m => ConcurrentMetaStoreM hnd m where
    -- | Write the meta-data structure to a certain page.
    putConcurrentMeta :: (Key k, Value v)
                      => hnd
                      -> ConcurrentMeta k v
                      -> m ()

    -- | Try to read the meta-data structure from a handle, or return 'Nothing'
    -- if the handle doesn't contain a meta page.
    readConcurrentMeta :: (Key k, Value v)
                       => hnd
                       -> Proxy k
                       -> Proxy v
                       -> m (Maybe (ConcurrentMeta k v))

