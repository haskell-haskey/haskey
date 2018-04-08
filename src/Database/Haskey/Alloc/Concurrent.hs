-- | The module implements an page allocator with page reuse and support for
-- multiple readers and serialized writers.
module Database.Haskey.Alloc.Concurrent (
  -- * Allocator
  ConcurrentDb(..)

  -- * Open, close and create databases
, ConcurrentHandles(..)
, concurrentHandles
, lockConcurrentDb
, unlockConcurrentDb
, createConcurrentDb
, openConcurrentDb
, closeConcurrentHandles

  -- * Manipulation and transactions
, module Database.Haskey.Alloc.Transaction
, transact
, transact_
, transactReadOnly

  -- * Storage requirements
, Root
, ConcurrentMeta(..)
, ConcurrentMetaStoreM(..)
) where

import Database.Haskey.Alloc.Concurrent.Internal.Database
import Database.Haskey.Alloc.Concurrent.Internal.Meta
import Database.Haskey.Alloc.Concurrent.Internal.Monad
import Database.Haskey.Alloc.Transaction
