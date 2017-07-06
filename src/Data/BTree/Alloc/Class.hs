
module Data.BTree.Alloc.Class where

import Data.BTree.Primitives

import Control.Applicative (Applicative)

--------------------------------------------------------------------------------

class (Applicative m, Monad m) => AllocM m where
    nodePageSize ::   (Key key, Value val)
                 =>   m (Height height -> Node height key val -> Int)
    maxPageSize  ::   m Int
    allocNode    ::   (Key key, Value val)
                 =>   Height height
                 ->   Node height key val
                 ->   m (NodeId height key val)
    readNode     ::   (Key key, Value val)
                 =>   Height height
                 ->   NodeId height key val
                 ->   m (Node height key val)
    freeNode     ::   Height height
                 ->   NodeId height key val
                 ->   m ()


--------------------------------------------------------------------------------
