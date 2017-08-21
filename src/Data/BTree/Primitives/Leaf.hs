module Data.BTree.Primitives.Leaf where

import Control.Applicative ((<$>))

import Data.Map (Map)
import qualified Data.Map as M

import Data.BTree.Primitives.Exception
import Data.BTree.Primitives.Index
import Data.BTree.Primitives.Key
import Data.BTree.Utils.List (safeLast)
import Data.BTree.Utils.Map (mapInits, mapSplitAt)

--------------------------------------------------------------------------------

-- | Split a leaf many times until the predicate is satisfied.
--
-- This function ensures that the for each returned leaf, the predicate is
-- satisfied, or returns 'Nothing' when it can't be satisfied.
splitLeafManyPred :: (Key key)
                    => (a -> Bool)
                    -> (Map key val -> a)
                    -> Map key val
                    -> Maybe (Index key a)
splitLeafManyPred p f = go
  where
    go items
        | indexEnc <- f items
        , p indexEnc
        = Just (singletonIndex indexEnc)
        | otherwise
        =  do
            left <- lstForWhich (p . f) inits'
            let right = items `M.difference` left
            mergeIndex (singletonIndex (f left))
                       (fst $ M.findMin right)
                       <$> go right
      where
        inits' = tail (mapInits items)

    lstForWhich :: (a -> Bool) -> [a] -> Maybe a
    lstForWhich g xs = safeLast $ takeWhile g xs

-- | Split a leaf many times.
--
-- This function ensures that the for each returned leaf, the amount of
-- items <= maxLeafItems (and >= minLeafItems, except when the original
-- leaf had less than minLeafItems items.
splitLeafMany :: Key key => Int -> (Map key val -> a) -> Map key val -> Index key a
splitLeafMany maxLeafItems f items
    | M.size items <= maxLeafItems
    = singletonIndex (f items)
    | M.size items <= 2*maxLeafItems
    , numLeft               <- div (M.size items) 2
    , (leftLeaf, rightLeaf) <- mapSplitAt numLeft items
    , Just ((key,_), _)     <- M.minViewWithKey rightLeaf
    = indexFromList [key] [f leftLeaf, f rightLeaf]
    | (keys, maps) <- split' items ([], [])
    = indexFromList keys (map f maps)
  where
    split' :: Key key => Map key val -> ([key], [Map key val]) -> ([key], [Map key val])
    split' m (keys, leafs)
        | M.size m > 2*maxLeafItems
        , (leaf, rem') <- mapSplitAt maxLeafItems m
        , (key, _)    <- M.findMin rem'
        = split' rem' (key:keys, leaf:leafs)
        | M.size m > maxLeafItems
        , numLeft       <- div (M.size m) 2
        , (left, right) <- mapSplitAt numLeft m
        , (key, _)      <- M.findMin right
        = split' M.empty (key:keys, right:(left:leafs))
        | M.null m
        = (reverse keys, reverse leafs)
        | otherwise
        = throw $ TreeAlgorithmError "splitLeafMany"
            "constraint violation, got a Map with <= maxLeafItems"

--------------------------------------------------------------------------------
