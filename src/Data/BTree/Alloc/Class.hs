{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-| A page allocator manages all physical pages. -}
module Data.BTree.Alloc.Class where

import Control.Applicative (Applicative)
import Control.Monad.State
import Control.Monad.Trans (lift)

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

data ReplacerState = First
                   | NotFirst

type Replacer m a = StateT ReplacerState m a

runReplacer :: Monad m => Replacer m a -> m a
runReplacer action = evalStateT action First

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
    writeNode    :: (Key key, Value val)
                 => NodeId height key val
                 -> Height height
                 -> Node height key val
                 -> m (NodeId height key val)

    {-| Read a node, and return a function that can write nodes.

       The returned function will:

       1. If the read node was most recently written in the same transaction,
          return a function that will first reuse the page of that node, and
          then allocate new pages for subsequent nodes.
       2. Otherwise, return a function that will allocate new pages for each

       The function can be ran using 'runReplacer'.
     -}
    replaceNode :: (Key key, Value val)
                => Height height
                -> NodeId height key val
                -> m (Node height key val, Node height key val -> Replacer m (NodeId height key val))
    replaceNode h nid = do
        (n, tx) <- readNodeTxId h nid
        curTx   <- currentTxId
        if curTx /= tx
            then -- Transaction ids are not the same: always allocate new nodes.
                 -- First free the page that was read, then return the 'always
                 -- allocate a new page'-writer.
                 freeNode h nid >> return (n, lift . allocNode h)
            else -- Transaciton ids are the same: reuse the page, don't free
                 -- it will still be in use!
                 return (n, replacer)
     where
         replacer n = get >>= \case
            NotFirst -> lift (allocNode h n)
            First    -> put NotFirst >> lift (writeNode nid h n)

    {-| Free the page belonging to the node. -}
    freeNode     ::  Height height
                 ->  NodeId height key val
                 ->  m ()

    {-| The id of the current write transaction. -}
    currentTxId :: m TxId

--------------------------------------------------------------------------------
