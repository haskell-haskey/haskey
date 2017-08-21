{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleContexts #-}
-- | A page allocator manages all physical pages.
module Data.BTree.Alloc.Class (
  -- * Classes
  AllocReaderM(..)
, AllocM(..)

  -- * Helpers
, arbitrarySearch
, calculateMaxKeySize
, calculateMaxValueSize
, ZeroEncoded(..)
) where

import Prelude hiding (max, min, pred)

import Control.Applicative (Applicative)

import Data.Binary
import Data.Typeable
import Data.Word (Word64)
import qualified Data.Map as M

import Data.BTree.Impure.Structures
import Data.BTree.Primitives

--------------------------------------------------------------------------------

-- | A page allocator that can read physical pages.
class (Applicative m, Monad m) => AllocReaderM m where
    -- | Read a page and return the actual node.
    readNode ::  (Key key, Value val)
             =>  Height height
             ->  NodeId height key val
             ->  m (Node height key val)

    -- | Read an overflow page.
    readOverflow :: (Value val)
                 => OverflowId
                 -> m val

-- | A page allocator that can write physical pages.
class AllocReaderM m => AllocM m where
    -- | A function that calculates the hypothetical size of a node, if it were
    -- to be written to a page (regardless of the maximum page size).
    nodePageSize ::  (Key key, Value val)
                 =>  m (Height height -> Node height key val -> PageSize)

    -- | The maximum page size the allocator can handle.
    maxPageSize  ::  m PageSize

    -- | Allocate a new page for a node, and write the node to the page.
    allocNode    :: (Key key, Value val)
                 => Height height
                 -> Node height key val
                 -> m (NodeId height key val)

    -- | Free the page belonging to the node.
    freeNode     ::  Height height
                 ->  NodeId height key val
                 ->  m ()

    -- | Allocate a new overflow page, and write the value to the page.
    allocOverflow :: (Value val)
                  => val
                  -> m OverflowId

    -- | Free an overflow page.
    freeOverflow :: OverflowId -> m ()

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
