{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | A pure in-memory B+-tree implementation.
module Data.BTree.Pure (
  -- * Structures
  module Data.BTree.Pure.Setup
, Tree(..)
, Node(..)

  -- * Manipulations
, empty
, singleton
, fromList
, insert
, insertMany
, delete

  -- * Lookup
, lookup
, findWithDefault
, member
, notMember

  -- * Properties
, null
, size

  -- * Folds
, foldrWithKey
, toList
) where

import Prelude hiding (lookup, null)

import Data.BTree.Primitives.Exception
import Data.BTree.Primitives.Height
import Data.BTree.Primitives.Index
import Data.BTree.Primitives.Key
import Data.BTree.Primitives.Leaf
import Data.BTree.Pure.Setup

import Data.Map (Map)
import Data.Maybe (isJust, isNothing, fromMaybe)
import Data.Monoid
import qualified Data.Foldable as F
import qualified Data.List as L
import qualified Data.Map as M

--------------------------------------------------------------------------------

-- | A pure B+-tree.
--
-- This is a simple wrapper around a root 'Node'. An empty tree is represented
-- by 'Nothing'. Otherwise it's 'Just' the root. The height is existentially
-- quantified.
data Tree key val where
    Tree :: !TreeSetup
         -> Maybe (Node height key val)
         -> Tree key val


-- | A node in a B+-tree.
--
-- Nodes are parameterized over the key and value types and are additionally
-- indexed by their height. All paths from the root to the leaves have the same
-- length. The height is the number of edges from the root to the leaves,
-- i.e. leaves are at height zero and index nodes increase the height.
data Node (height :: Nat) key val where
    Idx  :: { idxChildren   :: Index key (Node height key val)
            } -> Node ('S height) key val
    Leaf :: { leafItems     :: Map key val
            } -> Node 'Z key val

deriving instance (Show key, Show val) => Show (Node height key val)
deriving instance (Show key, Show val) => Show (Tree key val)

-- | The empty tree.
empty :: TreeSetup -> Tree key val
empty setup = Tree setup Nothing

-- | Construct a tree containg one element.
singleton :: Key k => TreeSetup -> k -> v -> Tree k v
singleton s k v = insert k v (empty s)

-- | /O(n*log n)/. Construct a B-tree from a list of key\/value pairs.
--
-- If the list contains duplicate keys, the last pair for a duplicate key is
-- kept.
fromList :: Key k => TreeSetup -> [(k,v)] -> Tree k v
fromList s = L.foldl' (flip $ uncurry insert) (empty s)

--------------------------------------------------------------------------------

-- | Insert a key-value pair into a tree.
--
-- When inserting a new entry, the leaf it is inserted to and the index nodes
-- on the path to the leaf potentially need to be split. Instead of returning
-- the outcome, 1 node or 2 nodes (with a discriminating key), we return an
-- 'Index' of these nodes.
--
-- If the key already existed in the tree, it is overwritten.
insert :: Key k => k -> v -> Tree k v -> Tree k v
insert k d (Tree setup (Just rootNode))
    | newRootIdx <- insertRec setup k d rootNode
    = case fromSingletonIndex newRootIdx of
          Just newRootNode ->
              -- The result from the recursive insert is a single node. Use
              -- this as a new root.
              Tree setup (Just newRootNode)
          Nothing          ->
              -- The insert resulted in a index with multiple nodes, i.e.
              -- the splitting propagated to the root. Create a new 'Idx'
              -- node with the index. This increments the height.
              Tree setup (Just (Idx newRootIdx))
insert k d (Tree setup Nothing)
    = -- Inserting into an empty tree creates a new singleton tree.
      Tree setup (Just (Leaf (M.singleton k d)))

insertRec :: Key key
          => TreeSetup
          -> key
          -> val
          -> Node height key val
          -> Index key (Node height key val)
insertRec setup key val (Idx children)
    | -- Punch a hole into the index at the sub-tree we recurse into.
      (ctx, child) <- valView key children
    , newChildIdx  <- insertRec setup key val child
    = -- Fill the hole with the resulting 'Index' from the recursive call
      -- and then check if the split needs to be propagated.
      splitIndex setup (putIdx ctx newChildIdx)
insertRec setup key val (Leaf items)
    = splitLeaf setup (M.insert key val items)

-- | Insert a bunch of key-value pairs simultaneously.
insertMany :: Key k => Map k v -> Tree k v -> Tree k v
insertMany kvs (Tree setup (Just rootNode))
    = fixUp setup $ insertRecMany setup kvs rootNode
insertMany kvs (Tree setup Nothing)
    = fixUp setup $ splitLeaf setup kvs

insertRecMany :: Key key
              => TreeSetup
              -> Map key val
              -> Node height key val
              -> Index key (Node height key val)
insertRecMany setup kvs (Idx idx)
    | M.null kvs
    = singletonIndex (Idx idx)
    | dist            <- distribute kvs idx
    = splitIndex setup (dist `bindIndex` uncurry (insertRecMany setup))
insertRecMany setup kvs (Leaf items)
    = splitLeaf setup (M.union kvs items)

-- | Fix up the root node of a tree.
--
-- Fix up the root node of a tree, where all other nodes are valid, but the
-- root node may contain more items than allowed. Do this by repeatedly
-- splitting up the root node.
fixUp :: Key key
      => TreeSetup
      -> Index key (Node height key val)
      -> Tree key val
fixUp setup idx = case fromSingletonIndex idx of
    Just newRootNode -> Tree setup (Just newRootNode)
    Nothing          -> fixUp setup (splitIndex setup idx)

--------------------------------------------------------------------------------

-- | /O(n)/. Fold key\/value pairs in the B-tree.
foldrWithKey :: forall k v w. (k -> v -> w -> w) -> w -> Tree k v -> w
foldrWithKey f z0 (Tree _ mbRoot) = maybe z0 (go z0) mbRoot
  where
    go :: w -> Node h k v -> w
    go z1 (Leaf items) = M.foldrWithKey f z1 items
    go z1 (Idx index)  = F.foldr (flip go) z1 index

-- | /O(n)/. Convert the B-tree to a sorted list of key\/value pairs.
toList :: Tree k v -> [(k,v)]
toList = foldrWithKey (\k v kvs -> (k,v):kvs) []

--------------------------------------------------------------------------------

-- | Delete a key-value pair from the tree.
delete :: Key k => k -> Tree k v -> Tree k v
delete _key (Tree setup Nothing)  = Tree setup Nothing
delete key  (Tree setup (Just rootNode)) = case deleteRec setup key rootNode of
    Idx index
      | Just childNode <- fromSingletonIndex index -> Tree setup (Just childNode)
    Leaf items
      | M.null items -> Tree setup Nothing
    newRootNode -> Tree setup (Just newRootNode)

deleteRec :: Key k
          => TreeSetup
          -> k
          -> Node n k v
          -> Node n k v
deleteRec setup key (Idx children)
    | childNeedsMerge, Just (rKey, rChild, rCtx) <- rightView ctx
    = Idx (putIdx rCtx (mergeNodes setup newChild rKey rChild))
    | childNeedsMerge, Just (lCtx, lChild, lKey) <- leftView ctx
    = Idx (putIdx lCtx (mergeNodes setup lChild lKey newChild))
    -- No left or right sibling? This is a constraint violation. Also
    -- this couldn't be the root because it would've been shrunk
    -- before.
    | childNeedsMerge
    = throw $ TreeAlgorithmError "deleteRec" 
        "constraint violation, found an index node with a single child"
    | otherwise = Idx (putVal ctx newChild)
  where
    (ctx, child)    = valView key children
    newChild        = deleteRec setup key child
    childNeedsMerge = nodeNeedsMerge setup newChild
deleteRec _ key (Leaf items)
    = Leaf (M.delete key items)

nodeNeedsMerge :: TreeSetup -> Node height key value -> Bool
nodeNeedsMerge setup = \case
    Idx children -> indexNumKeys children < minIdxKeys setup
    Leaf items   -> M.size items < minLeafItems setup

mergeNodes :: Key key
           => TreeSetup
           -> Node height key val
           -> key
           -> Node height key val
           -> Index key (Node height key val)
mergeNodes setup (Leaf leftItems) _middleKey (Leaf rightItems) =
    splitLeaf setup (leftItems <> rightItems)
mergeNodes setup (Idx leftIdx) middleKey (Idx rightIdx) =
    splitIndex setup (mergeIndex leftIdx middleKey rightIdx)

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

-- | Lookup a value in the tree.
lookup :: Key k => k -> Tree k v -> Maybe v
lookup _ (Tree _ Nothing) = Nothing
lookup k (Tree _ (Just n)) = lookupRec k n

-- | Lookup a value in the tree, or return a default.
findWithDefault :: Key k => v -> k -> Tree k v -> v
findWithDefault v k = fromMaybe v . lookup k

-- | Check whether a key is present in the tree.
member :: Key k => k -> Tree k v -> Bool
member k = isJust . lookup k

-- | Check whether a key is not present in the tree.
notMember :: Key k => k -> Tree k v -> Bool
notMember k = isNothing . lookup k

--------------------------------------------------------------------------------

-- | Check whether the tree is empty.
null :: Tree k v -> Bool
null (Tree _ n) = isNothing n

sizeNode :: Node n k v -> Int
sizeNode (Leaf items) = M.size items
sizeNode (Idx nodes)  = F.sum (fmap sizeNode nodes)

-- | The size of a tree.
size :: Tree k v -> Int
size (Tree _ Nothing) = 0
size (Tree _ (Just n)) = sizeNode n

--------------------------------------------------------------------------------

-- | Make a tree node foldable over its value.
instance F.Foldable (Tree key) where
    foldMap _ (Tree _ Nothing) = mempty
    foldMap f (Tree _ (Just n)) = F.foldMap f n

instance F.Foldable (Node height key) where
    foldMap f (Idx idx) =
        F.foldMap (F.foldMap f) idx

    foldMap f (Leaf items) = F.foldMap f items

--------------------------------------------------------------------------------

splitIndex :: TreeSetup
           -> Index key (Node height key val)
           -> Index key (Node ('S height) key val)
splitIndex setup = extendedIndex (maxIdxKeys setup) Idx

splitLeaf :: Key key
          => TreeSetup
          -> Map key val
          -> Index key (Node 'Z key val)
splitLeaf setup = splitLeafMany (maxLeafItems setup) Leaf

--------------------------------------------------------------------------------
