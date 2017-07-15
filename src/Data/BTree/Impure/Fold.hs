{-# LANGUAGE GADTs #-}
{-| Algorithms related to folding over an impure B+-tree. -}
module Data.BTree.Impure.Fold where

import Prelude hiding (foldr, foldl)

import Control.Applicative ((<$>))

import Data.Function (on)
import Data.List (sortBy)
import Data.Monoid (Monoid, (<>), mempty)
import qualified Data.Map as M
import qualified Data.Foldable as F

import Data.BTree.Alloc.Class
import Data.BTree.Impure.Structures
import Data.BTree.Primitives

--------------------------------------------------------------------------------

{-| Perform a right-associative fold over the tree. -}
foldr :: (AllocReaderM m, Key k, Value a)
      => (a -> b -> b) -> b -> Tree k a -> m b
foldr f = foldrM (\a b -> return (f a b))

{-| Perform a right-associative fold over the tree key-value pairs. -}
foldrWithKey :: (AllocReaderM m, Key k, Value a)
             => (k -> a -> b -> b) -> b -> Tree k a -> m b
foldrWithKey f = foldrWithKeyM (\k a b -> return (f k a b))

{-| Perform a monadic right-associative fold over the tree. -}
foldrM :: (AllocReaderM m, Key k, Value a)
       => (a -> b -> m b) -> b -> Tree k a -> m b
foldrM f = foldrWithKeyM (const f)

{-| Perform a monadic right-assiciative fold over the tree key-value pairs. -}
foldrWithKeyM :: (AllocReaderM m, Key k, Value a)
              => (k -> a -> b -> m b) -> b -> Tree k a -> m b
foldrWithKeyM _ x (Tree _ Nothing) = return x
foldrWithKeyM f x (Tree h (Just nid)) = foldrIdWithKeyM f x h nid

foldrIdWithKeyM :: (AllocReaderM m, Key k, Value a)
         => (k -> a -> b -> m b) -> b -> Height h -> NodeId h k a -> m b
foldrIdWithKeyM f x h nid = readNode h nid >>= foldrNodeWithKeyM f x h

foldrNodeWithKeyM :: (AllocReaderM m, Key k, Value a)
           => (k -> a -> b -> m b) -> b -> Height h -> Node h k a -> m b
foldrNodeWithKeyM f x _ (Leaf items) = M.foldrWithKey f' return items x
  where f' k a m z = f k a z >>= m
foldrNodeWithKeyM f x h (Idx idx) =
    F.foldrM (\nid x' -> foldrIdWithKeyM f x' (decrHeight h) nid) x idx

--------------------------------------------------------------------------------

{-| Map each value of the tree to a monoid, and combine the results. -}
foldMap :: (AllocReaderM m, Key k, Value a, Monoid c)
      => (a -> c) -> Tree k a -> m c
foldMap f = foldr ((<>) . f) mempty

{-| Convert an impure B+-tree to a list of key-value pairs.

   BUG: something is wrong with foldr, so sorting is necessary for now as a
   a work around. -}
toList :: (AllocReaderM m, Key k, Value a)
      => Tree k a -> m [(k, a)]
toList t = sortBy (compare `on` fst) <$> foldrWithKey (\k v xs -> (k, v):xs) [] t

--------------------------------------------------------------------------------
