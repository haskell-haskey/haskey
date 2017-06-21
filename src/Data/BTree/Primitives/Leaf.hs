
module Data.BTree.Primitives.Leaf where

import Data.BTree.Internal
import Data.BTree.Primitives.Key
import Data.Map (Map)
import qualified Data.Map as M

--------------------------------------------------------------------------------

{-| Split the mapping of a leave at the middle element. Eventually this should
    take the binary size of key value pairs into account than just the count.
    This function should only be called on a 'Map' with at least two elements.
    This in particular means it should not be called on the map of a root leaf
    with only one element and care has to be taken when leaves overflow.

    See also 'splitIndex' in "Data.BTree.Primitives.Index".
 -}
splitLeaf :: Key key => Map key val -> (Map key val, key, Map key val)
splitLeaf items
    | numLeft               <- div (M.size items) 2
    , (leftLeaf, rightLeaf) <- mapSplitAt numLeft items
    , Just ((key,_), _)     <- M.minViewWithKey rightLeaf
    = (leftLeaf, key, rightLeaf)
    | otherwise
    = error "splitLeaf: constraint violation, got a Map with with less than \
            \two elements"

{-| Split a leaf many times.

    This function ensures that the for each returned leaf, the amount of
    items <= maxLeafItems (and >= minLeafItems, except when the original
    leaf had less than minLeafItems items.
-}
splitLeafMany :: Key key => Int -> Map key val -> ([key], [Map key val])
splitLeafMany maxLeafItems m'
    | M.size m' > maxLeafItems = split' m' ([], [])
    | otherwise = ([], [m'])
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
        = error "splitLeafMany: constraint violation, got a Map with <= maxLeafItems"

--------------------------------------------------------------------------------
