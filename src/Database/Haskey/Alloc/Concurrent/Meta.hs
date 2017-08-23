{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}
-- | This module implements data structures and function related to the
-- metadata of the concurrent page allocator.
module Database.Haskey.Alloc.Concurrent.Meta where

import Data.Binary (Binary)
import Data.Proxy (Proxy)
import Data.Set as Set

import GHC.Generics (Generic)

import Data.BTree.Impure.Structures
import Data.BTree.Primitives

import Database.Haskey.Alloc.Concurrent.Environment
import Database.Haskey.Alloc.Concurrent.FreePages.Tree
import Database.Haskey.Alloc.Concurrent.Overflow
import Database.Haskey.Store

-- | Data type used to point to the most recent version of the meta data.
data CurrentMetaPage = Meta1 | Meta2

-- | Meta data of the page allocator.
data ConcurrentMeta k v = ConcurrentMeta {
    concurrentMetaRevision :: TxId
  , concurrentMetaDataNumPages :: S 'TypeData PageId
  , concurrentMetaIndexNumPages :: S 'TypeIndex PageId
  , concurrentMetaTree :: Tree k v
  , concurrentMetaDataFreeTree :: S 'TypeData FreeTree
  , concurrentMetaIndexFreeTree :: S 'TypeIndex FreeTree
  , concurrentMetaOverflowTree :: OverflowTree
  , concurrentMetaDataFreshUnusedPages :: S 'TypeData (Set DirtyFree)
  , concurrentMetaIndexFreshUnusedPages :: S 'TypeIndex (Set DirtyFree)
  } deriving (Generic)

deriving instance (Show k, Show v) => Show (ConcurrentMeta k v)

instance (Binary k, Binary v) => Binary (ConcurrentMeta k v) where

-- | A class representing the storage requirements of the page allocator.
--
-- A store supporting the page allocator should be an instance of this class.
class StoreM FilePath m => ConcurrentMetaStoreM m where
    -- | Write the meta-data structure to a certain page.
    putConcurrentMeta :: (Key k, Value v)
                      => FilePath
                      -> ConcurrentMeta k v
                      -> m ()

    -- | Try to read the meta-data structure from a handle, or return 'Nothing'
    -- if the handle doesn't contain a meta page.
    readConcurrentMeta :: (Key k, Value v)
                       => FilePath
                       -> Proxy k
                       -> Proxy v
                       -> m (Maybe (ConcurrentMeta k v))

