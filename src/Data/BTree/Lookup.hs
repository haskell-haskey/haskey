{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}

module Data.BTree.Lookup where

import Data.BTree.Alloc.Class
import Data.BTree.Primitives

import qualified Data.Foldable as F
import qualified Data.Map as M

--------------------------------------------------------------------------------

lookupRec :: forall m height key val. (AllocM m, Key key, Value val)
    => key
    -> Height height
    -> NodeId height key val
    -> m (Maybe val)
lookupRec k = fetchAndGo
  where
    fetchAndGo :: forall hgt.
        Height hgt ->
        NodeId hgt key val ->
        m (Maybe val)
    fetchAndGo hgt nid =
        readNode hgt nid >>= go hgt

    go :: forall hgt.
        Height hgt ->
        Node hgt key val ->
        m (Maybe val)
    go hgt (Idx children) = do
        let (_ctx,childId) = valView k children
        fetchAndGo (decrHeight hgt) childId
    go _hgt (Leaf items) =
        return $! M.lookup k items

--------------------------------------------------------------------------------

lookupTree :: forall m key val. (AllocM m, Key key, Value val)
    => key
    -> Tree key val
    -> m (Maybe val)
lookupTree k tree
    | Tree
      { treeHeight = height
      , treeRootId = Just rootId
      } <- tree
    = lookupRec k height rootId
    | Tree
      { treeRootId = Nothing
      } <- tree
    = return Nothing

--------------------------------------------------------------------------------

foldr :: (AllocM m, Key k, Value a)
      => (m a -> m b -> m b) -> m b -> Tree k a -> m b
foldr _ x (Tree _ Nothing) = x
foldr f x (Tree h (Just nid)) = foldrId f x h nid

foldrId :: (AllocM m, Key k, Value a)
        => (m a -> m b -> m b) -> m b -> Height h -> NodeId h k a -> m b
foldrId f x h nid = readNode h nid >>= foldrNode f x h

foldrNode :: (AllocM m, Key k, Value a)
          => (m a -> m b -> m b) -> m b -> Height h -> Node h k a -> m b
foldrNode f x _ (Leaf items) = M.foldr f x (return <$> items)
foldrNode f x h (Idx idx) = F.foldr (\nid x' -> foldrId f x' (decrHeight h) nid) x idx
