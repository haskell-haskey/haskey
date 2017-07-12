{-| A page allocator manages all physical pages. -}
module Data.BTree.Alloc.Class where

import Control.Applicative (Applicative)

import Data.Int

import Data.BTree.Impure.Structures
import Data.BTree.Primitives

--------------------------------------------------------------------------------

{-| The physical size of a page, in bytes. -}
type PageSize = Int32

{-| A page allocator that manages physical pages. -}
class (Applicative m, Monad m) => AllocM m where
    {-| A function that calculates the hypothetical size of a node, if it were
       to be written to a page (regardless of the maximum page size).
     -}
    nodePageSize ::  (Key key, Value val)
                 =>  m (Height height -> Node height key val -> PageSize)

    {-| The maximum page size the allocator can handle. -}
    maxPageSize  ::  m PageSize

    {-| Allocate a new page for a node, and write the node to the page. -}
    allocNode    ::  (Key key, Value val)
                 =>  Height height
                 ->  Node height key val
                 ->  m (NodeId height key val)

    {-| Read a page and return the actual node. -}
    readNode     ::  (Key key, Value val)
                 =>  Height height
                 ->  NodeId height key val
                 ->  m (Node height key val)

    {-| Free the page belonging to the node. -}
    freeNode     ::  Height height
                 ->  NodeId height key val
                 ->  m ()

--------------------------------------------------------------------------------
