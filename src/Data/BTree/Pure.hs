{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.BTree.Pure where

import           Data.BTree.Primitives.Height
import           Data.BTree.Primitives.Index
import           Data.BTree.Primitives.Key
import           Data.BTree.Primitives.Leaf
import           Data.BTree.TwoThree

import qualified Data.Foldable as F
import           Data.Int
import qualified Data.List as L
import           Data.Map (Map)
import           Data.Maybe (isJust, isNothing, fromMaybe)
import           Data.Monoid
import           Prelude hiding (lookup, null)
import qualified Data.Map as M
import qualified Data.Vector as V

--------------------------------------------------------------------------------

{-| A node in a B+-tree.

    Nodes are parameterized over the key and value types and are additionally
    indexed by their height. All paths from the root to the leaves have the same
    length. The height is the number of edges from the root to the leaves,
    i.e. leaves are at height zero and index nodes increase the height.
-}
data Node (height :: Nat) key val where
    Idx  :: { idxChildren   :: Index key (Node height key val)
            } -> Node ('S height) key val
    Leaf :: { leafItems     :: Map key val
            } -> Node 'Z key val

{-| A pure B+-tree.

    This is a simple wrapper around a root 'Node'. An empty tree is represented
    by 'Nothing'. Otherwise it's 'Just' the root. The height is existentially
    quantified.
-}
data Tree key val where
    Tree :: Maybe (Node height key val)
         -> Tree key val


deriving instance (Show key, Show val) => Show (Node height key val)
deriving instance (Show key, Show val) => Show (Tree key val)

empty :: Tree key val
empty = Tree Nothing

{-| Check whether a given tree is valid. -}
validTree :: Ord key => Tree key val -> Bool
validTree (Tree Nothing) = True
validTree (Tree (Just (Leaf items))) = M.size items <= maxLeafItems
validTree (Tree (Just (Idx idx))) =
    validIndexSize 1 maxIdxKeys idx && F.all validNode idx

{-| Check whether a (non-root) node is valid. -}
validNode :: Ord key => Node height key val -> Bool
validNode (Leaf items) =
    M.size items >= minLeafItems && M.size items <= maxLeafItems
validNode (Idx idx) =
    validIndexSize minIdxKeys maxIdxKeys idx && F.all validNode idx

--------------------------------------------------------------------------------

checkSplitIdx :: Key key =>
   Index key (Node height key val) ->
   Index key (Node ('S height) key val)
checkSplitIdx idx
    -- In case the branching fits in one index node we create it.
    | V.length (indexKeys idx) <= maxIdxKeys
    = indexFromList [] [Idx idx]
    -- Otherwise we split the index node.
    | (leftIdx, middleKey, rightIdx) <- splitIndex idx
    = indexFromList [middleKey] [Idx leftIdx, Idx rightIdx]

checkSplitLeaf :: Key key => Map key val -> Index key (Node 'Z key val)
checkSplitLeaf items
    | M.size items <= maxLeafItems
    = indexFromList [] [Leaf items]
    | (leftItems, middleKey, rightItems) <- splitLeaf items
    = indexFromList [middleKey] [Leaf leftItems, Leaf rightItems]

checkSplitIdxMany :: Key key
                  => Index key (Node height key val)
                  -> Index key (Node ('S height) key val)
checkSplitIdxMany idx
    | V.length (indexKeys idx) <= maxIdxKeys
    = indexFromList [] [Idx idx]
    | (keys, idxs) <- splitIndexMany maxIdxKeys idx
    = indexFromList keys (map Idx idxs)

checkSplitLeafMany :: Key key => Map key val -> Index key (Node 'Z key val)
checkSplitLeafMany items
    | M.size items <= maxLeafItems
    = indexFromList [] [Leaf items]
    | (keys, leafs) <- splitLeafMany maxLeafItems items
    = indexFromList keys (map Leaf leafs)

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
    -> Node height key val
    -> Index key (Node height key val)
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

insertRecMany ::
       Key key
    => Map key val
    -> Node height key val
    -> Index key (Node height key val)
insertRecMany kvs (Idx idx)
    | dist            <- distribute kvs idx
    = checkSplitIdxMany (dist `bindIndex` uncurry insertRecMany)

insertRecMany kvs (Leaf items)
    = checkSplitLeafMany (M.union kvs items)

{-| Insert a bunch of key-value pairs simultaneously. -}
insertMany :: Key k => Map k v -> Tree k v -> Tree k v
insertMany kvs (Tree (Just rootNode))
    | newRootIdx <- insertRecMany kvs rootNode
    = case fromSingletonIndex newRootIdx of
          Just newRootNode ->
              -- The result from the recursive insert is a single node. Use
              -- this as a new root.
              fixUp $ Tree (Just newRootNode)
          Nothing          ->
              -- The insert resulted in a index with multiple nodes, i.e.
              -- the splitting propagated to the root. Create a new 'Idx'
              -- node with the index. This increments the height.
              fixUp $ Tree (Just (Idx newRootIdx))
insertMany kvs (Tree Nothing)
    = fixUp $ Tree (Just (Leaf kvs))

{-| Fix up the root node of a tree.

    Fix up the root node of a tree, where all other nodes are valid, but the
    root node may contain more items than allowed. Do this by repeatedly
    splitting up the root node.
-}
fixUp :: Key k => Tree k v -> Tree k v
fixUp (Tree Nothing) = Tree Nothing
fixUp (Tree (Just (Leaf items)))
    | newRootIdx <- checkSplitLeafMany items
    = case fromSingletonIndex newRootIdx of
        Just newRootNode -> Tree (Just newRootNode)
        Nothing          -> fixUp $ Tree (Just (Idx newRootIdx))
fixUp (Tree (Just (Idx idx)))
    | newRootIdx <- checkSplitIdxMany idx
    = case fromSingletonIndex newRootIdx of
        Just newRootNode -> Tree (Just newRootNode)
        Nothing          -> fixUp $ Tree (Just (Idx newRootIdx))


{-| /O(n*log n)/. Construct a B-tree from a list of key\/value pairs.

    If the list contains duplicate keys, the last pair for a duplicate key is
    kept.
-}
fromList :: Key k => [(k,v)] -> Tree k v
fromList = L.foldl' (flip $ uncurry insert) empty

{-| /O(n)/. Fold key\/value pairs in the B-tree.
-}
foldrWithKey :: forall k v w. (k -> v -> w -> w) -> w -> Tree k v -> w
foldrWithKey f z0 (Tree mbRoot) = case mbRoot of
    Nothing   -> z0
    Just root -> go z0 root
  where
    go :: w -> Node h k v -> w
    go z1 (Leaf items) = M.foldrWithKey f z1 items
    go z1 (Idx index)  = F.foldr (flip go) z1 index

{-| /O(n)/. Convert the B-tree to a sorted list of key\/value pairs.
-}
toList :: Tree k v -> [(k,v)]
toList = foldrWithKey (\k v kvs -> (k,v):kvs) []

--------------------------------------------------------------------------------

nodeNeedsMerge :: Node height key value -> Bool
nodeNeedsMerge Idx  { idxChildren = children } =
    V.length (indexKeys children) < minIdxKeys
nodeNeedsMerge Leaf { leafItems   = items }    =
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

deleteRec ::
       Key k
    => k
    -> Node n k v
    -> Node n k v
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
    -> Node height key val
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

sizeNode :: Node n k v -> Int
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

-- | Make a tree node foldable over its value.

instance F.Foldable (Tree key) where
    foldMap _ (Tree Nothing) = mempty
    foldMap f (Tree (Just n)) = F.foldMap f n

instance F.Foldable (Node height key) where
    foldMap f (Idx Index { indexNodes = nodes }) =
        F.foldMap (F.foldMap f) nodes

    foldMap f (Leaf items) = F.foldMap f items


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
