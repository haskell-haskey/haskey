{-# LANGUAGE GADTs #-}
module Data.BTree.Fold where

import Prelude hiding (foldr, foldl)

import Data.BTree.Alloc.Class
import Data.BTree.Primitives

import Data.Monoid (Monoid, (<>), mempty)

import qualified Data.Foldable as F

--------------------------------------------------------------------------------

{-| Perform a right-associative fold over the tree. -}
foldr :: (AllocM m, Key k, Value a)
      => (a -> b -> b) -> b -> Tree k a -> m b
foldr f = foldrM (\a b -> return (f a b))

{-| Perform a monadic right-associative fold over the tree. -}
foldrM :: (AllocM m, Key k, Value a)
       => (a -> b -> m b) -> b -> Tree k a -> m b
foldrM _ x (Tree _ Nothing) = return x
foldrM f x (Tree h (Just nid)) = foldrIdM f x h nid

foldrIdM :: (AllocM m, Key k, Value a)
         => (a -> b -> m b) -> b -> Height h -> NodeId h k a -> m b
foldrIdM f x h nid = readNode h nid >>= foldrNodeM f x h

foldrNodeM :: (AllocM m, Key k, Value a)
           => (a -> b -> m b) -> b -> Height h -> Node h k a -> m b
foldrNodeM f x _ (Leaf items) = F.foldrM f x items
foldrNodeM f x h (Idx idx) = F.foldrM (\nid x' -> foldrIdM f x' (decrHeight h) nid) x idx

--------------------------------------------------------------------------------

foldMap :: (AllocM m, Key k, Value a, Monoid c)
      => (a -> c) -> Tree k a -> m c
foldMap f = foldr ((<>) . f) mempty

toList :: (AllocM m, Key k, Value a)
      => Tree k a -> m [a]
toList = foldr (:) []

--------------------------------------------------------------------------------
