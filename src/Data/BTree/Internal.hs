
module Data.BTree.Internal where

import           Data.Map (Map)
import qualified Data.Map as M
import           Data.Vector (Vector)
import qualified Data.Vector as V

--------------------------------------------------------------------------------

mapSplitAt :: Eq k => Int -> Map k v -> (Map k v, Map k v)
mapSplitAt i m
    | (l,r) <- splitAt i (M.toList m)
    = (M.fromAscList l, M.fromAscList r)

--------------------------------------------------------------------------------

vecUncons :: Vector a -> Maybe (a, Vector a)
vecUncons v
    | V.null v  = Nothing
    | otherwise = Just (V.unsafeHead v, V.unsafeTail v)

vecUnsnoc :: Vector a -> Maybe (Vector a, a)
vecUnsnoc v
    | V.null v  = Nothing
    | otherwise = Just (V.unsafeInit v, V.unsafeLast v)

isStrictlyIncreasing :: Ord key => Vector key -> Bool
isStrictlyIncreasing ks = case vecUncons ks of
    Just (h,t) ->
      snd $ V.foldl' (\(lb,res) next -> (next, res && lb < next)) (h, True) t
    Nothing    -> True

--------------------------------------------------------------------------------
