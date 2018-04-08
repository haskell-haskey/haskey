{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}
-- | This module implements data structures and function related to the
-- metadata of the concurrent page allocator.
module Database.Haskey.Alloc.Concurrent.Internal.Meta where

import Data.Binary (Binary)
import Data.Proxy (Proxy)
import Data.Typeable (Typeable)

import GHC.Generics (Generic)

import Data.BTree.Impure.Structures
import Data.BTree.Primitives

import Database.Haskey.Alloc.Concurrent.Internal.Environment
import Database.Haskey.Alloc.Concurrent.Internal.FreePages.Tree
import Database.Haskey.Alloc.Concurrent.Internal.Overflow
import Database.Haskey.Store

-- | User-defined data root stored inside 'ConcurrentMeta'.
--
-- This can be a user-defined collection of 'Tree' roots.
class Value root => Root root where

instance (Key k, Value v) => Root (Tree k v) where

-- | Data type used to point to the most recent version of the meta data.
data CurrentMetaPage = Meta1 | Meta2

-- | Meta data of the page allocator.
--
-- The @root@ type parameter should be a user-defined collection of 'Tree'
-- roots, instantiating the 'Root' type class.
--
-- To store store a single tree, use @ConcurrentMeta (Tree k v)@.
data ConcurrentMeta root = ConcurrentMeta {
    concurrentMetaRevision :: TxId
  , concurrentMetaDataNumPages :: S 'TypeData PageId
  , concurrentMetaIndexNumPages :: S 'TypeIndex PageId
  , concurrentMetaRoot :: root
  , concurrentMetaDataFreeTree :: S 'TypeData FreeTree
  , concurrentMetaIndexFreeTree :: S 'TypeIndex FreeTree
  , concurrentMetaOverflowTree :: OverflowTree
  , concurrentMetaDataCachedFreePages :: S 'TypeData [FreePage]
  , concurrentMetaIndexCachedFreePages :: S 'TypeIndex [FreePage]
  } deriving (Generic, Typeable)

deriving instance (Show root) => Show (ConcurrentMeta root)

instance (Binary root) => Binary (ConcurrentMeta root) where

-- | A class representing the storage requirements of the page allocator.
--
-- A store supporting the page allocator should be an instance of this class.
class StoreM FilePath m => ConcurrentMetaStoreM m where
    -- | Write the meta-data structure to a certain page.
    putConcurrentMeta :: Root root
                      => FilePath
                      -> ConcurrentMeta root
                      -> m ()

    -- | Try to read the meta-data structure from a handle, or return 'Nothing'
    -- if the handle doesn't contain a meta page.
    readConcurrentMeta :: Root root
                       => FilePath
                       -> Proxy root
                       -> m (Maybe (ConcurrentMeta root))

