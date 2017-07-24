{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RankNTypes #-}
{-| The module implements an page allocator with page reuse and support for
   multiple readers and serialized writers.
 -}
module Data.BTree.Alloc.Concurrent (
  -- * Allocator
  ConcurrentDb(..)

  -- * Manipulation and transactions
, module Data.BTree.Alloc.Transaction
, transact
, transact_
, transactReadOnly
) where

import STMContainers.Map (Map)

import Data.BTree.Alloc.Class
import Data.BTree.Alloc.PageReuse (PageReuseDb, PageReuseMetaStoreM)
import Data.BTree.Alloc.Transaction
import Data.BTree.Impure.Structures
import Data.BTree.Primitives
import qualified Data.BTree.Alloc.PageReuse as PageReuse

{-| An active page allocator with concurrency and page reuse. -}
data ConcurrentDb hnd k v = ConcurrentDb
    { concurrentDbPageReuse :: PageReuseDb hnd k v
    , concurrentDbReaders :: Map TxId Integer
    }

--------------------------------------------------------------------------------

{-| Execute a write transaction, with a result. -}
transact :: (PageReuseMetaStoreM hnd m, Key key, Value val)
         => (forall n. AllocM n => Tree key val -> n (Transaction key val a))
         -> ConcurrentDb hnd key val -> m (ConcurrentDb hnd key val, a)
transact act db
    | ConcurrentDb
      { concurrentDbPageReuse = pageReuseDb
      } <- db
    = do
        (db', v) <- PageReuse.transact act pageReuseDb
        return (db { concurrentDbPageReuse = db' }, v)

{-| Execute a write transaction, without a result. -}
transact_ :: (PageReuseMetaStoreM hnd m, Key key, Value val)
          => (forall n. AllocM n => Tree key val -> n (Transaction key val ()))
          -> ConcurrentDb hnd key val -> m (ConcurrentDb hnd key val)
transact_ act db = fst <$> transact act db

{-| Execute a read-only transaction. -}
transactReadOnly :: (PageReuseMetaStoreM hnd m)
                 => (forall n. AllocReaderM n => Tree key val -> n a)
                 -> ConcurrentDb hnd key val -> m a
transactReadOnly act db
    | ConcurrentDb
      { concurrentDbPageReuse = pageReuseDb
      } <- db
    = PageReuse.transactReadOnly act pageReuseDb
