{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-| Algorithms related to looking up key-value pairs in an impure B+-tree. -}
module Data.BTree.Impure.Lookup where

import qualified Data.Map as M

import Data.BTree.Alloc.Class
import Data.BTree.Impure.Structures
import Data.BTree.Primitives

--------------------------------------------------------------------------------

lookupRec :: forall m height key val. (AllocReaderM m, Key key, Value val)
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

{-| Lookup a value in an impure B+-tree. -}
lookupTree :: forall m key val. (AllocReaderM m, Key key, Value val)
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

{-| The minimal key of the map, returns 'Nothing' if the map is empty. -}
lookupMinTree :: (AllocReaderM m, Key key, Value val)
              => Tree key val
              -> m (Maybe (key, val))
lookupMinTree tree
    | Tree { treeRootId = Nothing } <- tree = return Nothing
    | Tree { treeHeight = height
           , treeRootId = Just rootId } <- tree
    = lookupMinRec height rootId
  where
    lookupMinRec :: (AllocReaderM m, Key key, Value val)
                 => Height height
                 -> NodeId height key val
                 -> m (Maybe (key, val))
    lookupMinRec h nid = readNode h nid >>= \case
        Idx children -> let (_, childId) = valViewMin children in
                        lookupMinRec (decrHeight h) childId
        Leaf items -> return $! lookupMin items

    lookupMin m | M.null m  = Nothing
                | otherwise = Just $! M.findMin m

--------------------------------------------------------------------------------
