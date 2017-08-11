-- | An impure B+-tree implementation.
--
-- This module contains the implementation of a B+-tree that is backed by a
-- page allocator (see "Data.BTree.Alloc") and a storage back-end (see
-- "Data.BTree.Store").
module Data.BTree.Impure (
  -- * Structures
  Tree(..)
, Node(..)

  -- * Construction
, empty
, fromList
, fromMap

  -- * Manipulation
, insertTree
, insertTreeMany
, deleteTree

  -- * Lookup
, lookupTree
, lookupMinTree

  -- * Folds
, foldr
, foldrM
, foldrWithKey
, foldrWithKeyM
, foldMap
, toList
) where

import Prelude hiding (foldr, foldMap)

import Data.Map (Map)
import qualified Data.Map as M

import Data.BTree.Alloc.Class
import Data.BTree.Impure.Delete (deleteTree)
import Data.BTree.Impure.Structures (Tree(..), Node(..))
import Data.BTree.Impure.Fold (foldr, foldrM, foldrWithKey, foldrWithKeyM, foldMap, toList)
import Data.BTree.Impure.Insert (insertTree, insertTreeMany)
import Data.BTree.Impure.Lookup (lookupTree, lookupMinTree)

import Data.BTree.Primitives

-- | Create an empty tree.
empty :: Tree k v
empty = Tree zeroHeight Nothing

-- | Create a tree from a list.
fromList :: (AllocM m, Key k, Value v)
         => [(k, v)]
         -> m (Tree k v)
fromList = fromMap . M.fromList

-- | Create a tree from a map.
fromMap :: (AllocM m, Key k, Value v)
        => Map k v
        -> m (Tree k v)
fromMap kvs = insertTreeMany kvs empty
