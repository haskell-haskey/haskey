{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.BTree.Delete where

import           Data.BTree.Alloc.Class
import           Data.BTree.Insert
import           Data.BTree.Primitives
import           Data.BTree.TwoThree

import qualified Data.Map as M
import           Data.Monoid
import           Data.Traversable (traverse)

--------------------------------------------------------------------------------

nodeNeedsMerge :: Node height key val -> Bool
nodeNeedsMerge (Idx children) =
    indexNumKeys children < minIdxKeys
nodeNeedsMerge (Leaf items) =
    M.size items < minLeafItems

mergeNodes :: Key key
    => Node height key val
    -> key
    -> Node height key val
    -> Index key (Node height key val)
mergeNodes (Leaf leftItems) _middleKey (Leaf rightItems) =
    checkSplitLeaf (leftItems <> rightItems)
mergeNodes (Idx leftIdx) middleKey (Idx rightIdx) =
    checkSplitIdx (mergeIndex leftIdx middleKey rightIdx)

--------------------------------------------------------------------------------

deleteRec :: forall height key val m. (AllocM m, Key key, Value val)
    => key
    -> Height height
    -> NodeId height key val
    -> m (Node height key val)
deleteRec key = fetchAndGo
  where
    fetchAndGo :: forall hgt. Height hgt
        -> NodeId hgt key val
        -> m (Node hgt key val)
    fetchAndGo hgt nid = do
        node <- readNode hgt nid
        freeNode hgt nid
        recurse hgt node

    recurse :: forall hgt. Height hgt
       -> Node hgt key val
       -> m (Node hgt key val)
    recurse hgt (Idx children) = do
        let (ctx, childId) = valView key children
            subHeight      = decrHeight hgt
        newChild <- fetchAndGo subHeight childId
        let childNeedsMerge = nodeNeedsMerge newChild
        if | childNeedsMerge, Just (rKey, rChildId, rCtx) <- rightView ctx -> do
                 rChild <- readNode subHeight rChildId
                 freeNode subHeight rChildId
                 newChildren <- traverse (allocNode subHeight)
                                  (mergeNodes newChild rKey rChild)
                 return (Idx (putIdx rCtx newChildren))
           | childNeedsMerge, Just (lCtx, lChildId, lKey) <- leftView ctx -> do
                 lChild <- readNode subHeight lChildId
                 freeNode subHeight lChildId
                 newChildIds <- traverse (allocNode subHeight)
                                  (mergeNodes lChild lKey newChild)
                 return (Idx (putIdx lCtx newChildIds))
           -- No left or right sibling? This is a constraint violation. Also
           -- this couldn't be the root because it would've been shrunk
           -- before.
           | childNeedsMerge ->
                 error "deleteRec: constraint violation, found an index \
                       \node with a single child"
           | otherwise -> do
                 newChildId <- allocNode subHeight newChild
                 return (Idx (putVal ctx newChildId))
    recurse _hgt (Leaf items) =
        return (Leaf (M.delete key items))

--------------------------------------------------------------------------------

deleteTree :: (AllocM m, Key key, Value val)
    => key
    -> Tree key val
    -> m (Tree key val)
deleteTree k tree
    | Tree
      { treeRootId = Nothing
      } <- tree
    = return tree
    | Tree
      { treeHeight = height
      , treeRootId = Just rootId
      } <- tree
    = do
          newRootNode <- deleteRec k height rootId
          case newRootNode of
              Idx index
                | Just childNodeId <- fromSingletonIndex index ->
                  return $! Tree
                      { treeHeight = decrHeight height
                      , treeRootId = Just childNodeId
                      }
              Leaf items
                | M.null items ->
                  return $! Tree
                      { treeHeight = zeroHeight
                      , treeRootId = Nothing
                      }
              _ -> do
                  newRootNodeId <- allocNode height newRootNode
                  return $! Tree
                        { treeHeight = height
                        , treeRootId = Just newRootNodeId
                        }

--------------------------------------------------------------------------------
