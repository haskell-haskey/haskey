
module Data.BTree.Primitives.Leaf where

import Data.BTree.Internal
import Data.BTree.Primitives.Index
import Data.BTree.Primitives.Key

import Data.Binary (Binary)
import Data.Map (Map)
import qualified Data.Binary as B
import qualified Data.ByteString.Lazy as BS
import qualified Data.Map as M

import Data.Int

--------------------------------------------------------------------------------

{-| Split a leaf many times.
gt
    This function ensures that the for each returned leaf, the amount of
    items <= maxLeafItems (and >= minLeafItems, except when the original
    leaf had less than minLeafItems items.
-}
splitLeafManyBinary :: (Binary a, Key key)
                    => Int
                    -> (Map key val -> a)
                    -> Map key val
                    -> Index key a
splitLeafManyBinary maxPageSize f = go
  where
    go items
        | itemsSize <= fromIntegral maxPageSize || M.size items <= 1
        = singletonIndex (f items)
        | (right, left) <-
            maybe (head inits', items `M.difference` head inits')
                  (\left -> (left, items `M.difference` left))
                  (lstForWhich (splitHalf itemsSize) inits')
        = mergeIndex (go left) (fst $ M.findMin right) (go right)
      where
        itemsSize = encSize items
        inits' = mapInits items

        splitHalf origSize m = encSize m <= origSize `div` 2

    encSize = BS.length . B.encode . f
    lstForWhich :: (a -> Bool) -> [a] -> Maybe a
    lstForWhich g xs = case span g xs of ([], _) -> Nothing
                                         (x,  _) -> Just $ last x

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
        = error "splitLeafMany: constraint violation, got a Map with <= maxLeafItems"
--------------------------------------------------------------------------------
