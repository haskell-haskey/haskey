{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}

module Data.BTree.Primitives.Index where

import           Data.BTree.Internal

import           Control.Applicative ((<$>))
import           Control.Monad.Identity (runIdentity)

import           Data.Binary (Binary)
import           Data.Foldable (Foldable)
import qualified Data.Map as M
import           Data.Traversable (Traversable)
import           Data.Monoid
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Data.Vector.Binary ()

import GHC.Generics (Generic)

--------------------------------------------------------------------------------

{-| The 'Index' encodes the internal structure of an index node.

    The index is abstracted over the type 'node' of sub-trees. The keys and
    nodes are stored in separate 'Vector's and the keys are sorted in strictly
    increasing order. There should always be one more sub-tree than there are
    keys. Hence structurally the smallest 'Index' has one sub-tree and no keys,
    but a valid B+-tree index node will have at least two sub-trees and one key.
 -}
data Index key node = Index
    { indexKeys  :: !(Vector key)
    , indexNodes :: !(Vector node)
    }
  deriving (Eq, Functor, Foldable, Generic, Show, Traversable)

instance (Binary k, Binary n) => Binary (Index k n) where

{-| Validate the key/node count invariant of an index.  -}
validIndex :: Ord key => Index key node -> Bool
validIndex (Index keys nodes) =
    V.length keys + 1 == V.length nodes &&
    isStrictlyIncreasing keys

{-| Validate the size of an index. -}
validIndexSize :: Ord key => Int -> Int -> Index key node -> Bool
validIndexSize minIdxKeys maxIdxKeys idx@(Index keys _) =
    validIndex idx && V.length keys >= minIdxKeys && V.length keys <= maxIdxKeys

{-| Split an index node.

    This function splits an index node into two new nodes around the middle
    element and returns the resulting indices and the key separating them.
    Eventually this should take the binary size of serialized keys and sub-tree
    pointers into account. See also 'splitLeaf' in
    "Data.BTree.Primitives.Leaf".
-}
splitIndex :: Index key node -> (Index key node, key, Index key node)
splitIndex Index { indexKeys = keys, indexNodes = nodes }
    | numLeftKeys                       <- div (V.length keys) 2
    , numLeftNodes                      <- numLeftKeys + 1
    , (leftKeys, middleKeyAndRightKeys) <- V.splitAt numLeftKeys keys
    , Just (middleKey,rightKeys)        <- vecUncons middleKeyAndRightKeys
    , (leftNodes, rightNodes)           <- V.splitAt numLeftNodes nodes
    = ( Index
        { indexKeys  = leftKeys
        , indexNodes = leftNodes
        }
      , middleKey
      , Index
        { indexKeys  = rightKeys
        , indexNodes = rightNodes
        }
      )
    | otherwise
    = error "splitIndex: empty Index"

{-| Split an index node many times.

    This function splits an index node into a list of valid nodes, such that
    each returned node has less than maxIdxKeys nodes (but more than minIdxKeys).

    This function raises an exception when the index should not be split, i.e.
    when the the amount of keys in the index <= maxIdxKeys.
-}
splitIndexMany :: Int -> Index key node -> ([key], [Index key node])
splitIndexMany maxIdxKeys idx = split' idx ([], [])
  where
    split' ::  Index key node -> ([key], [Index key node]) -> ([key], [Index key node])
    split' Index {indexKeys = keysToSplit, indexNodes = nodesToSplit } (keys, children)
        | V.length nodesToSplit > 2*maxIdxItems
        , (leftKeys, middleAndRightKeys) <- V.splitAt maxIdxKeys keysToSplit
        , Just (middleKey, rightKeys)    <- vecUncons middleAndRightKeys
        , (leftNodes, rightNodes)        <- V.splitAt maxIdxItems nodesToSplit
        = split' (Index rightKeys rightNodes)
                 (middleKey:keys, Index leftKeys leftNodes : children)
        | V.length nodesToSplit > maxIdxItems
        , (left, key, right) <- splitIndex $ Index keysToSplit nodesToSplit
        = (reverse (key:keys), reverse $ right:(left:children))
        | otherwise
        = error $ "splitIndexMany: constraint violation, got index with " ++
                  " length indexNodes <= maxIdxItems"

    maxIdxItems = maxIdxKeys + 1

{-| Merge two indices.

    Merge two indices 'leftIndex', 'rightIndex' given a discriminating key
    'middleKey', i.e. such that '∀ (k,v) ∈ leftIndex. k < middleKey' and
    '∀ (k,v) ∈ rightIndex. middleKey <= k'.

    'mergeIndex' is a partial inverse of splitIndex, i.e.
    prop> splitIndex is == (left,mid,right) => mergeIndex left mid right == is
-}
mergeIndex :: Index key val -> key -> Index key val -> Index key val
mergeIndex leftIndex middleKey rightIndex = Index
    { indexKeys = indexKeys leftIndex <>
                  V.singleton middleKey <>
                  indexKeys rightIndex
    , indexNodes = indexNodes leftIndex <>
                  indexNodes rightIndex
    }

{-| Create an index from key-value lists.

    The internal invariants of the 'Index' data structure apply. That means
    there is one more value than there are keys and keys are ordered in strictly
    increasing order.
-}
indexFromList :: [key] -> [val] -> Index key val
indexFromList ks vs = Index (V.fromList ks) (V.fromList vs)

{-| Create an index with a single value.
-}
singletonIndex :: val -> Index key val
singletonIndex = Index V.empty . V.singleton

{-| Test if the index consists of a single value.

    Returns the element if the index is a singleton. Otherwise fails.
-}
fromSingletonIndex :: Index key val -> Maybe val
fromSingletonIndex idx
    | vals <- indexNodes idx
    , V.length vals == 1
    = Just $! V.unsafeHead vals
    | otherwise
    = Nothing

--------------------------------------------------------------------------------

{-| Bind an index -}
bindIndex :: Index k a -> (a -> Index k b) -> Index k b
bindIndex idx f = runIdentity $ bindIndexM idx (return . f)

bindIndexM :: (Functor m, Monad m) => Index k a -> (a -> m (Index k b)) -> m (Index k b)
bindIndexM (Index ks is) f = (merge' . vecUncons) <$> V.mapM f is
 where
    merge' Nothing = Index ks V.empty
    merge' (Just (i, itail)) = V.foldl' (uncurry . mergeIndex) i (V.zip ks itail)

--------------------------------------------------------------------------------

{-| Representation of one-hole contexts of 'Index'.

    Just one val removes. All keys are present.

    V.length leftVals  == V.length lefyKeys
    V.length rightVals == V.length rightKeys
-}
data IndexCtx key val = IndexCtx
    { indexCtxLeftKeys  :: !(Vector key)
    , indexCtxRightKeys :: !(Vector key)
    , indexCtxLeftVals  :: !(Vector val)
    , indexCtxRightVals :: !(Vector val)
    }
  deriving (Functor, Foldable, Show, Traversable)

putVal :: IndexCtx key val -> val -> Index key val
putVal ctx val = Index
    { indexKeys  = indexCtxLeftKeys ctx <>
                   indexCtxRightKeys ctx
    , indexNodes = indexCtxLeftVals ctx <>
                   V.singleton val <>
                   indexCtxRightVals ctx
    }

putIdx :: IndexCtx key val -> Index key val -> Index key val
putIdx ctx idx = Index
    { indexKeys  = indexCtxLeftKeys ctx <>
                   indexKeys idx <>
                   indexCtxRightKeys ctx
    , indexNodes = indexCtxLeftVals ctx <>
                   indexNodes idx <>
                   indexCtxRightVals ctx
    }

valView :: Ord key => key -> Index key val -> (IndexCtx key val, val)
valView key Index { indexKeys = keys, indexNodes = vals }
    | (leftKeys,rightKeys)       <- V.span (<=key) keys
    , n                          <- V.length leftKeys
    , (leftVals,valAndRightVals) <- V.splitAt n vals
    , Just (val,rightVals)       <- vecUncons valAndRightVals
    = ( IndexCtx
        { indexCtxLeftKeys  = leftKeys
        , indexCtxRightKeys = rightKeys
        , indexCtxLeftVals  = leftVals
        , indexCtxRightVals = rightVals
        },
        val
      )
    | otherwise
    = error "valView: empty Index"

{-| Distribute a map of key-value pairs over an index. -}
distribute :: Ord k => M.Map k v -> Index k node -> Index k (M.Map k v, node)
distribute kvs Index { indexKeys = keys, indexNodes = nodes }
    | a <- V.imap rangeTail          (Nothing `V.cons` V.map Just keys)
    , b <- V.map (uncurry rangeHead) (V.zip (V.map Just keys `V.snoc` Nothing) a)
    = Index { indexKeys = keys
            , indexNodes = b }
  where
    rangeTail idx Nothing    = (kvs, nodes V.! idx)
    rangeTail idx (Just key) = (takeWhile' (>= key) kvs, nodes V.! idx)
    rangeHead Nothing (tail', node)    = (tail', node)
    rangeHead (Just key) (tail', node)  = (takeWhile' (< key) tail', node)

    takeWhile' :: (k -> Bool) -> M.Map k v -> M.Map k v
    takeWhile' p = fst . M.partitionWithKey (\k _ -> p k)

leftView :: IndexCtx key val -> Maybe (IndexCtx key val, val, key)
leftView ctx = do
  (leftVals, leftVal) <- vecUnsnoc (indexCtxLeftVals ctx)
  (leftKeys, leftKey) <- vecUnsnoc (indexCtxLeftKeys ctx)
  return (ctx { indexCtxLeftKeys = leftKeys
              , indexCtxLeftVals = leftVals
              }, leftVal, leftKey)

rightView :: IndexCtx key val -> Maybe (key, val, IndexCtx key val)
rightView ctx = do
  (rightVal, rightVals) <- vecUncons (indexCtxRightVals ctx)
  (rightKey, rightKeys) <- vecUncons (indexCtxRightKeys ctx)
  return (rightKey, rightVal,
          ctx { indexCtxRightKeys = rightKeys
              , indexCtxRightVals = rightVals
              })

--------------------------------------------------------------------------------
