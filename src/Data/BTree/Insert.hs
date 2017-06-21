{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.BTree.Insert where

import           Data.BTree.Alloc.Class
import           Data.BTree.Primitives
import           Data.BTree.TwoThree

import           Data.Map (Map)
import qualified Data.Map as M
import           Data.Traversable (traverse)
import qualified Data.Vector as V

--------------------------------------------------------------------------------

checkSplitIdx :: Key key
    => Index key (NodeId height key val)
    -> Index key (Node ('S height) key val)
checkSplitIdx idx
    -- In case the branching fits in one index node we create it.
    | V.length (indexKeys idx) <= maxIdxKeys
    = indexFromList [] [Idx idx]
    -- Otherwise we split the index node.
    | (leftIdx, middleKey, rightIdx) <- splitIndex idx
    = indexFromList [middleKey] [Idx leftIdx, Idx rightIdx]

checkSplitLeaf :: Key key
    => Map key val
    -> Index key (Node 'Z key val)
checkSplitLeaf items
    | M.size items <= maxLeafItems
    = indexFromList [] [Leaf items]
    | (leftItems, middleKey, rightItems) <- splitLeaf items
    = indexFromList [middleKey] [Leaf leftItems, Leaf rightItems]

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
        traverse (allocNode hgt) (checkSplitIdx (putIdx ctx newChildIdx))
    recurse hgt (Leaf items) =
        traverse (allocNode hgt) (checkSplitLeaf (M.insert k v items))

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

--------------------------------------------------------------------------------
