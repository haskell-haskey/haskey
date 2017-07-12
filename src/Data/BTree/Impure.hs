{-| An impure B+-tree implementation.
 -
 - This module contains the implementation of a B+-tree that is backed by a
 - page allocator (see "Data.BTree.Alloc") and a storage back-end (see
 - "Data.BTree.Store").
 -}
module Data.BTree.Impure (
  -- * Structures
  Tree(..)
, Node(..)

  -- * Manipulation
, empty
, insertTree
, insertTreeMany
, deleteTree

  -- * Lookup
, lookupTree

  -- * Folds
, foldr
, foldrM
, foldrWithKey
, foldrWithKeyM
, foldMap
, toList
) where

import Prelude hiding (foldr, foldMap)

import Data.BTree.Impure.Delete (deleteTree)
import Data.BTree.Impure.Structures (Tree(..), Node(..))
import Data.BTree.Impure.Fold (foldr, foldrM, foldrWithKey, foldrWithKeyM, foldMap, toList)
import Data.BTree.Impure.Insert (insertTree, insertTreeMany)
import Data.BTree.Impure.Lookup (lookupTree)

import Data.BTree.Primitives (zeroHeight)

{-| Create an empty tree. -}
empty :: Tree k v
empty = Tree zeroHeight Nothing
