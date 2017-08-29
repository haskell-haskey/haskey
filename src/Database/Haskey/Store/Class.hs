{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances #-}
-- | A storage back-end manages physical storage of pages.
module Database.Haskey.Store.Class (
  -- * Class
  StoreM(..)

  -- * Helpers
, arbitrarySearch
, calculateMaxKeySize
, calculateMaxValueSize
, ZeroEncoded(..)
) where

import Prelude hiding (max, min, pred)

import Control.Applicative (Applicative)
import Control.Monad.Trans
import Control.Monad.Trans.Reader (ReaderT)
import Control.Monad.Trans.State (StateT)

import Data.Binary (Binary(..), Get)
import Data.Proxy
import Data.Typeable (Typeable)
import Data.Word (Word8, Word64)
import qualified Data.Map as M

import Data.BTree.Impure
import Data.BTree.Impure.Structures
import Data.BTree.Primitives

--------------------------------------------------------------------------------

-- | A storage back-end that can store and fetch physical pages.
class (Applicative m, Monad m) => StoreM hnd m | m -> hnd where
    -- | Open a database handle for reading and writing.
    openHandle :: hnd -> m ()

    -- | Obtain a lock on the given handle, so no other process can access it.
    lockHandle :: hnd -> m ()

    -- | Release the lock on the given handle, so other processes can access it.
    releaseHandle :: hnd -> m ()

    -- | Flush the contents of a handle to disk (or other storage).
    flushHandle :: hnd -> m ()

    -- | Close a database handle.
    closeHandle :: hnd -> m ()

    -- | Remove a handle from the storage back-end.
    removeHandle :: hnd -> m ()

    -- | A function that calculates the hypothetical size of a node, if it were
    -- to be written to a page (regardless of the maximum page size).
    nodePageSize :: (Key key, Value val)
                 => m (Height height -> Node height key val -> PageSize)

    -- | The maximum page size the allocator can handle.
    maxPageSize  :: m PageSize

    -- | Get the maximum key size
    --
    -- The default implementation will repeatedly call 'calculateMaxKeySize'.
    -- You might want to cache this value in your own implementation.
    maxKeySize :: m Word64
    maxKeySize =  do
        f    <- nodePageSize
        fmax <- maxPageSize
        return $ calculateMaxKeySize fmax (f zeroHeight)

    -- | Get the maximum value size
    --
    -- The default implementation will repeatedly call 'calculateMaxValueSize'.
    -- You might want to cache this value in your own implementation.
    maxValueSize :: m Word64
    maxValueSize = do
        f    <- nodePageSize
        key  <- maxKeySize
        fmax <- maxPageSize
        return $ calculateMaxValueSize fmax key (f zeroHeight)

    -- | Read a page and return the actual node and the transaction id when the
    -- node was written.
    getNodePage  :: (Key key, Value val)
                 => hnd
                 -> Height height
                 -> Proxy key
                 -> Proxy val
                 -> NodeId height key val
                 -> m (Node height key val)

    -- | Write a node to a physical page.
    putNodePage  :: (Key key, Value val)
                 => hnd
                 -> Height height
                 -> NodeId height key val
                 -> Node height key val
                 -> m ()

    -- | Read a value from an overflow page
    getOverflow :: (Value val)
                => hnd
                -> Proxy val
                -> m val

    -- | Write a value to an overflow page
    putOverflow :: (Value val)
                => hnd
                -> val
                -> m ()

    -- | List overflow pages in the specific overflow directory.
    --
    -- The result should include **AT LEAST** the handles in the specified
    -- directory, but it may contain more handles, even handles that do not
    -- belong to an overflow page.
    listOverflows :: hnd -> m [hnd]


instance StoreM hnd m => StoreM hnd (StateT s m) where
    openHandle    = lift.             openHandle
    lockHandle    = lift.             lockHandle
    releaseHandle = lift.             releaseHandle
    flushHandle   = lift.             flushHandle
    closeHandle   = lift.             closeHandle
    removeHandle  = lift.             closeHandle
    nodePageSize  = lift              nodePageSize
    maxPageSize   = lift              maxPageSize
    maxKeySize    = lift              maxKeySize
    maxValueSize  = lift              maxValueSize
    getNodePage   = ((((lift.).).).). getNodePage
    putNodePage   = (((lift.).).).    putNodePage
    getOverflow   = (lift.).          getOverflow
    putOverflow   = (lift.).          putOverflow
    listOverflows = lift.             listOverflows

instance StoreM hnd m => StoreM hnd (ReaderT s m) where
    openHandle    = lift.             openHandle
    lockHandle    = lift.             lockHandle
    releaseHandle = lift.             releaseHandle
    flushHandle   = lift.             flushHandle
    closeHandle   = lift.             closeHandle
    removeHandle  = lift.             closeHandle
    nodePageSize  = lift              nodePageSize
    maxPageSize   = lift              maxPageSize
    maxKeySize    = lift              maxKeySize
    maxValueSize  = lift              maxValueSize
    getNodePage   = ((((lift.).).).). getNodePage
    putNodePage   = (((lift.).).).    putNodePage
    getOverflow   = (lift.).          getOverflow
    putOverflow   = (lift.).          putOverflow
    listOverflows = lift.             listOverflows

--------------------------------------------------------------------------------

-- | Calculate the maximum key size.
--
-- Return the size for which at least 4 key-value pairs with keys and values of
-- that size can fit in a leaf node.
calculateMaxKeySize :: PageSize
                    -- ^ Maximum pages size
                    -> (Node 'Z ZeroEncoded ZeroEncoded -> PageSize)
                    -- ^ Function that calculates the page size of a node
                    -> Word64
                    -- ^ Maximum key size
calculateMaxKeySize fmax f = arbitrarySearch 2 pred fmax
  where
    pred n = f (Leaf $ kvs n)
    kvs n = M.fromList
        [(ZeroEncoded n i, RawValue $ ZeroEncoded n i) | i <- [1..4]]

-- | Calculate the maximum value size.
--
-- Return the size for which at least 4 key-value pairs of the specified
-- maximum key size and values of the returned size can fit in a leaf node.
-- that size can fit in a leaf node.
calculateMaxValueSize :: PageSize
                      -- ^ Maximum page size
                      -> Word64
                      -- ^ Maximum key size
                      -> (Node 'Z ZeroEncoded ZeroEncoded -> PageSize)
                      -- ^ Function that calculates the page size of a node
                      -> Word64
                      -- ^ Maximum value size
calculateMaxValueSize fmax keySize f = arbitrarySearch 2 pred fmax
  where
    pred n = f (Leaf $ kvs n)
    kvs n = M.fromList
        [(ZeroEncoded keySize i, RawValue $ ZeroEncoded n i) | i <- [1..4]]

-- | Search an arbitrary number, less than a limit, greater than a starting
-- value.
arbitrarySearch :: (Ord v, Integral n) => n -> (n -> v) -> v -> n
arbitrarySearch start f fmax = go start
  where
    go n =
        let s = f n in
        if s == fmax
        then n
        else if s > fmax
             then search (n `quot` 2) n
             else go (n*2)

    search min max
        | max - min == 1 = min
        | otherwise
        =
        let c = min + ((max - min) `quot` 2)
            s = f c in
        if s == fmax
        then c
        else if s > fmax
             then search min c
             else search c max

-- | Data type which encodes the integer using a variable amount of NULL or ONE
-- bytes.
data ZeroEncoded = ZeroEncoded { getZeroEncoded :: Word64
                               , getZeroEncodedValue :: Word64 }
                 deriving (Eq, Ord, Show, Typeable)


instance Binary ZeroEncoded where
    put (ZeroEncoded 0 _) = error "must be >0"
    put (ZeroEncoded 1 0) = put (255 :: Word8)
    put (ZeroEncoded 1 _) = error "value too large"
    put (ZeroEncoded n v) = put byte >> put (ZeroEncoded (n-1) v')
      where
        byte = fromIntegral $ v `rem` 255 :: Word8
        v' = v `quot` 255

    get = do
        byte <- get :: Get Word8
        case byte of
            0 -> return (ZeroEncoded 1 0)
            _ -> do
                next <- get
                return $ ZeroEncoded (getZeroEncoded next + 1) 0

instance Key ZeroEncoded where
instance Value ZeroEncoded where

--------------------------------------------------------------------------------
