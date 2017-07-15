{-# LANGUAGE ConstraintKinds #-}
{-| A page allocator manages all physical pages. -}
module Data.BTree.Alloc.Class where

import Control.Applicative (Applicative)

import Data.BTree.Impure.Structures
import Data.BTree.Primitives

--------------------------------------------------------------------------------

{-| A page allocator that can both read and write physical pages. -}
type AllocM m = (AllocReaderM m, AllocWriterM m)

{-| A page allocator that can read physical pages. -}
class (Applicative m, Monad m) => AllocReaderM m where
    {-| Read a page and return the actual node. -}
    readNode ::  (Key key, Value val)
             =>  Height height
             ->  NodeId height key val
             ->  m (Node height key val)

{-| A page allocator that can write physical pages. -}
class (Applicative m, Monad m) => AllocWriterM m where
    {-| A function that calculates the hypothetical size of a node, if it were
       to be written to a page (regardless of the maximum page size).
     -}
    nodePageSize ::  (Key key, Value val)
                 =>  m (Height height -> Node height key val -> PageSize)

    {-| The maximum page size the allocator can handle. -}
    maxPageSize  ::  m PageSize

    {-| Read a page and return the actual node and the transaction id this
       node was written at. -}
    readNodeTxId :: (Key key, Value val)
                 => Height height
                 -> NodeId height key val
                 -> m (Node height key val, TxId)

    {-| Allocate a new page for a node, and write the node to the page. -}
    allocNode    :: (Key key, Value val)
                 => Height height
                 -> Node height key val
                 -> m (NodeId height key val)

    {-| Write the a node to an existing page.  -}
    replaceNode  :: (Key key, Value val)
                 => NodeId height key val
                 -> Height height
                 -> Node height key val
                 -> m (NodeId height key val)

    {-| Read a node, map a function, and write the result, either to a new page
       or by replacing the contents of a dirty page that was written to already
       in the same transaction. -}
    writeNode :: (Key key, Value val)
              => Height height
              -> NodeId height key val
              -> (Node height key val -> m (Node height key val))
              -> m (NodeId height key val)
    writeNode h nid f = do
        (n, tx) <- readNodeTxId h nid
        curTx   <- currentTxId
        newNode <- f n
        if curTx == tx then replaceNode nid h newNode
                       else allocNode h newNode

    {-| Free the page belonging to the node. -}
    freeNode     ::  Height height
                 ->  NodeId height key val
                 ->  m ()

    {-| The id of the current write transaction. -}
    currentTxId :: m TxId

--------------------------------------------------------------------------------
