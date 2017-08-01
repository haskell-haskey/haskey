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

import Data.BTree.Alloc.Concurrent.Database
import Data.BTree.Alloc.Concurrent.Meta
import Data.BTree.Alloc.Transaction
