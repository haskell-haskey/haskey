{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.BTree.Insert where

import           Data.BTree.Alloc.Class
import           Data.BTree.Primitives

import           Data.Map (Map)
import qualified Data.Map as M
import           Data.Traversable (traverse)

--------------------------------------------------------------------------------

splitIndex :: (AllocM m, Key key, Value val) =>
   Height ('S height) ->
   Index key (NodeId height key val) ->
   m (Index key (Node ('S height) key val))
splitIndex h index = do
    m <- maxPageSize
    nodePageSize' <- nodePageSize
    let binPred n = nodePageSize' h n <= m
    case extendIndexPred binPred Idx index of
        Just extIndex -> return extIndex
        Nothing       -> error "Splitting failed!? Underflow "

splitLeaf :: (AllocM m, Key key, Value val) =>
    Map key val ->
    m (Index key (Node 'Z key val))
splitLeaf items = do
    m <- maxPageSize
    nodePageSize' <- nodePageSize
    let binPred n = nodePageSize' zeroHeight n <= m
    case splitLeafManyPred binPred Leaf items of
        Just v  -> return v
        Nothing -> error $ "Split leaf: underflow " ++ show items

--------------------------------------------------------------------------------

insertRec :: forall m height key val. (AllocM m, Key key, Value val)
    => key
    -> val
    -> Height height
    -> NodeId height key val
    -> m (Index key (NodeId height key val))
insertRec k v = fetch
  where
    fetch :: forall hgt.
           Height hgt
        -> NodeId hgt key val
        -> m (Index key (NodeId hgt key val))
    fetch hgt nid = do
        node <- readNode hgt nid
        freeNode hgt nid
        recurse hgt node

    recurse :: forall hgt.
           Height hgt
        -> Node hgt key val
        -> m (Index key (NodeId hgt key val))
    recurse hgt (Idx children) = do
        let (ctx,childId) = valView k children
        newChildIdx <- fetch (decrHeight hgt) childId
        newChildren <- splitIndex hgt (putIdx ctx newChildIdx)
        traverse (allocNode hgt) newChildren
    recurse hgt (Leaf items) =
        traverse (allocNode hgt) =<< splitLeaf (M.insert k v items)

insertRecMany :: forall m height key val. (AllocM m, Key key, Value val)
    => Height height
    -> Map key val
    -> NodeId height key val
    -> m (Index key (NodeId height key val))
insertRecMany h kvs nid
    | M.null kvs = return (singletonIndex nid)
    | otherwise = do
    n <- readNode h nid
    freeNode h nid
    case n of
        Idx idx -> do
            let dist = distribute kvs idx
            newIndex    <- dist `bindIndexM` uncurry (insertRecMany (decrHeight h))
            newChildren <- splitIndex h newIndex
            traverse (allocNode h) newChildren
        Leaf items ->
            traverse (allocNode h) =<< splitLeaf (M.union kvs items)

--------------------------------------------------------------------------------

insertTree :: (AllocM m, Key key, Value val)
    => key
    -> val
    -> Tree key val
    -> m (Tree key val)
insertTree key val tree
    | Tree
      { treeHeight = height
      , treeRootId = Just rootId
      } <- tree
    = do
          newRootIdx <- insertRec key val height rootId
          case fromSingletonIndex newRootIdx of
              Just newRootId ->
                  return $! Tree
                      { treeHeight = height
                      , treeRootId = Just newRootId
                      }
              Nothing -> do
                  -- Root got split, so allocate a new root node.
                  let newHeight = incrHeight height
                  newRootId <- allocNode newHeight Idx
                      { idxChildren = newRootIdx }
                  return $! Tree
                      { treeHeight = newHeight
                      , treeRootId = Just newRootId
                      }
    | Tree
      { treeRootId = Nothing
      } <- tree
    = do  -- Allocate new root node
          newRootId <- allocNode zeroHeight Leaf
              { leafItems = M.singleton key val
              }
          return $! Tree
              { treeHeight = zeroHeight
              , treeRootId = Just newRootId
              }

insertTreeMany :: (AllocM m, Key key, Value val)
    => Map key val
    -> Tree key val
    -> m (Tree key val)
insertTreeMany kvs tree
    | Tree
      { treeHeight = height
      , treeRootId = Just rootId
      } <- tree
    = do
        newRootIdx <- insertRecMany height kvs rootId
        fixUp height newRootIdx
    | Tree { treeRootId = Nothing } <- tree
    = do
        idx <- traverse (allocNode zeroHeight) =<< splitLeaf kvs
        fixUp zeroHeight $! idx

{-| Fix up the root node of a tree.

    Fix up the root node of a tree, where all other nodes are valid, but the
    root node may contain more items than allowed. Do this by repeatedly
    splitting up the root node.
-}
fixUp :: (AllocM m, Key key, Value val)
       => Height height
       -> Index key (NodeId height key val)
       -> m (Tree key val)
fixUp h idx = case fromSingletonIndex idx of
    Just newRootNid ->
        return $! Tree { treeHeight = h
                       , treeRootId = Just newRootNid }
    Nothing -> do
        let newHeight = incrHeight h
        children     <- splitIndex newHeight idx
        childrenNids <- traverse (allocNode newHeight) children
        fixUp newHeight $! childrenNids

--------------------------------------------------------------------------------
