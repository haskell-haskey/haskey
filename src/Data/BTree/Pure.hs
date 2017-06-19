{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE StandaloneDeriving #-}

module Data.BTree.Pure where

import           Data.BTree.Primitives.Height
import           Data.BTree.Primitives.Key
import           Data.BTree.Primitives.Index
import           Data.BTree.Primitives.Leaf

import qualified Data.Foldable as F
import           Data.Int
import           Data.Map (Map)
import           Data.Maybe (isJust, isNothing, fromMaybe)
import           Data.Monoid
import           Prelude hiding (lookup, null)
import qualified Data.Map as M
import qualified Data.Vector as V

--------------------------------------------------------------------------------

-- Setup of a 2-3 tree.
minFanout :: Int
minFanout = 2

maxFanout :: Int
maxFanout = 2*maxFanout-1

minIdxKeys :: Int
minIdxKeys = minFanout - 1

maxIdxKeys :: Int
maxIdxKeys = maxFanout - 1

minLeafItems :: Int
minLeafItems = maxFanout

maxLeafItems :: Int
maxLeafItems = 2*maxFanout-1

--------------------------------------------------------------------------------

{-| A node in a B+-tree.

    Nodes are parameterized over the key and value types and are additionally
    indexed by their height. All paths from the root to the leaves have the same
    length. The height is the number of edges from the root to the leaves,
    i.e. leaves are at height zero and index nodes increase the height.
-}
data Node key val (height :: Nat) where
    Idx  :: { idxChildren   :: Index key (Node key val height)
            } -> Node key val ('S height)
    Leaf :: { leafItems     :: Map key val
            } -> Node key val 'Z

data Tree key val where
    Tree :: -- An empty tree is represented by 'Nothing'. Otherwise it's 'Just'
            -- the root of the tree. The height is existentially quantified.
            Maybe (Node key val height)
         -> Tree key val


deriving instance (Show key, Show val) => Show (Node key val height)
deriving instance (Show key, Show val) => Show (Tree key val)

empty :: Tree key val
empty = Tree Nothing

--------------------------------------------------------------------------------

checkSplitIdx :: Key key =>
   Index key (Node key val height) ->
   Index key (Node key val ('S height))
checkSplitIdx idx
    -- In case the branching fits in one index node we create it.
    | V.length (indexKeys idx) <= maxIdxKeys
    = indexFromList [] [Idx idx]
    -- Otherwise we split the index node.
    | (leftIdx, middleKey, rightIdx) <- splitIndex idx
    = indexFromList [middleKey] [Idx leftIdx, Idx rightIdx]

checkSplitLeaf :: Key key => Map key val -> Index key (Node key val 'Z)
checkSplitLeaf items
    | M.size items <= maxLeafItems
    = indexFromList [] [Leaf items]
    | (leftLeaf, middleKey, rightLeaf) <- splitLeaf items
    = indexFromList [middleKey] [Leaf leftLeaf, Leaf rightLeaf]

--------------------------------------------------------------------------------

{-| Insert a key-value pair into a tree.

    When inserting a new entry, the leaf it is inserted to and the index nodes
    on the path to the leaf potentially need to be split. Instead of returning
    the outcome, 1 node or 2 nodes (with a discriminating key), we return an
    'Index' of these nodes.

    If the key already existed in the tree, it is overwritten.
-}
insertRec ::
       Key key
    => key
    -> val
    -> Node key val height
    -> Index key (Node key val height)
insertRec key val (Idx children)
    | -- Punch a hole into the index at the sub-tree we recurse into.
      (ctx, child) <- valView key children
    , newChildIdx  <- insertRec key val child
    = -- Fill the hole with the resulting 'Index' from the recursive call
      -- and then check if the split needs to be propagated.
      checkSplitIdx (putIdx ctx newChildIdx)
insertRec key val (Leaf items)
    = checkSplitLeaf (M.insert key val items)

insert :: Key k => k -> v -> Tree k v -> Tree k v
insert k d (Tree (Just rootNode))
    | newRootIdx <- insertRec k d rootNode
    = case fromSingletonIndex newRootIdx of
          Just newRootNode ->
              -- The result from the recursive insert is a single node. Use
              -- this as a new root.
              Tree (Just newRootNode)
          Nothing          ->
              -- The insert resulted in a index with multiple nodes, i.e.
              -- the splitting propagated to the root. Create a new 'Idx'
              -- node with the index. This increments the height.
              Tree (Just (Idx newRootIdx))
insert k d (Tree Nothing)
    = -- Inserting into an empty tree creates a new singleton tree.
      Tree (Just (Leaf (M.singleton k d)))

fromList :: Key k => [(k,v)] -> Tree k v
fromList = foldr (uncurry insert) empty

--------------------------------------------------------------------------------

nodeNeedsMerge :: Node key value height -> Bool
nodeNeedsMerge Idx  { idxChildren = children } =
    V.length (indexKeys children) < minIdxKeys
nodeNeedsMerge Leaf { leafItems   = items }    =
    M.size items < minLeafItems

mergeNodes :: Key key
    => Node key val height
    -> key
    -> Node key val height
    -> Index key (Node key val height)
mergeNodes (Leaf leftItems) _middleKey (Leaf rightItems) =
    checkSplitLeaf (leftItems <> rightItems)
mergeNodes (Idx leftIdx) middleKey (Idx rightIdx) =
    checkSplitIdx (mergeIndex leftIdx middleKey rightIdx)

deleteRec ::
       Key k
    => k
    -> Node k v n
    -> Node k v n
deleteRec key (Idx children)
    | childNeedsMerge, Just (rKey, rChild, rCtx) <- rightView ctx
    = Idx (putIdx rCtx (mergeNodes newChild rKey rChild))
    | childNeedsMerge, Just (lCtx, lChild, lKey) <- leftView ctx
    = Idx (putIdx lCtx (mergeNodes lChild lKey newChild))
    -- No left or right sibling? This is a constraint violation. Also
    -- this couldn't be the root because it would've been shrunk
    -- before.
    | childNeedsMerge
    = error "deleteRec: constraint violation, found an index \
             \node with a single child"
    | otherwise = Idx (putVal ctx newChild)
  where
    (ctx, child)    = valView key children
    newChild        = deleteRec key child
    childNeedsMerge = nodeNeedsMerge newChild
deleteRec key (Leaf items)
    = Leaf (M.delete key items)

delete :: Key k => k -> Tree k v -> Tree k v
delete _key (Tree Nothing)  = Tree Nothing
delete key  (Tree (Just rootNode))
    | newRootNode <- deleteRec key rootNode
    = case newRootNode of
          Idx index
              | Just childNode <- fromSingletonIndex index ->
                Tree (Just childNode)
          Leaf items
              | M.null items ->
                Tree Nothing
          _ -> Tree (Just newRootNode)

--------------------------------------------------------------------------------

lookupRec :: Key key
    => key
    -> Node key val height
    -> Maybe val
lookupRec key (Idx children)
    | (_, childNode) <- valView key children
    = lookupRec key childNode
lookupRec key (Leaf items)
    = M.lookup key items

lookup :: Key k => k -> Tree k v -> Maybe v
lookup _ (Tree Nothing) = Nothing
lookup k (Tree (Just n)) = lookupRec k n

--------------------------------------------------------------------------------

singleton :: Key k => k -> v -> Tree k v
singleton k v = insert k v empty

null :: Tree k v -> Bool
null (Tree n) = isNothing n

sizeNode :: Node k v n -> Int
sizeNode (Leaf items) = M.size items
sizeNode (Idx nodes)  = F.sum (fmap sizeNode nodes)

size :: Tree k v -> Int
size (Tree Nothing) = 0
size (Tree (Just n)) = sizeNode n

member :: Key k => k -> Tree k v -> Bool
member k = isJust . lookup k

notMember :: Key k => k -> Tree k v -> Bool
notMember k = isNothing . lookup k

findWithDefault :: Key k => v -> k -> Tree k v -> v
findWithDefault v k = fromMaybe v . lookup k

--------------------------------------------------------------------------------

test3 :: Tree Int64 String
test3 = insert 3 "bar" empty

test32 :: Tree Int64 String
test32 = insert 2 "foo" test3

test325 :: Tree Int64 String
test325 = insert 5 "baz" test32

test3254 :: Tree Int64 String
test3254 = insert 4 "oof" test325

test3254d3 :: Tree Int64 String
test3254d3 = delete 3 test3254

test3254d4 :: Tree Int64 String
test3254d4 = delete 4 test3254

test3254d5 :: Tree Int64 String
test3254d5 = delete 5 test3254

--------------------------------------------------------------------------------
