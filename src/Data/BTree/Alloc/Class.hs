-- | A page allocator manages all physical pages.
module Data.BTree.Alloc.Class (
  -- * Classes
  AllocReaderM(..)
, AllocM(..)
) where

import Prelude hiding (max, min, pred)

import Control.Applicative (Applicative)

import Data.Word (Word64)

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

    -- | The maximum key size
    maxKeySize :: m Word64

    -- | The maximum value size
    maxValueSize :: m Word64

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

--------------------------------------------------------------------------------
