
module Data.BTree.Alloc.Class where

import Data.BTree.Primitives

import Control.Applicative (Applicative)

--------------------------------------------------------------------------------

class (Applicative m, Monad m) => AllocM m where
    nodeSize    ::   (Key key, Value val)
                =>   Node height key val -> m Int
    maxNodeSize ::   m Int
    allocNode   ::   (Key key, Value val)
                =>   Height height
                ->   Node height key val
                ->   m (NodeId height key val)
    readNode    ::   (Key key, Value val)
                =>   Height height
                ->   NodeId height key val
                ->   m (Node height key val)
    freeNode    ::   Height height
                ->   NodeId height key val
                ->   m ()


--------------------------------------------------------------------------------
