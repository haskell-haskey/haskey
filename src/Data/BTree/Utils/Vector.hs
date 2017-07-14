module Data.BTree.Utils.Vector where

import Data.List (inits)
import Data.Vector (Vector)
import qualified Data.Vector as V

vecUncons :: Vector a -> Maybe (a, Vector a)
vecUncons v
    | V.null v  = Nothing
    | otherwise = Just (V.unsafeHead v, V.unsafeTail v)

vecUnsnoc :: Vector a -> Maybe (Vector a, a)
vecUnsnoc v
    | V.null v  = Nothing
    | otherwise = Just (V.unsafeInit v, V.unsafeLast v)

vecInits :: Vector a -> Vector (Vector a)
vecInits = V.map V.fromList . V.fromList . inits . V.toList

isStrictlyIncreasing :: Ord key => Vector key -> Bool
isStrictlyIncreasing ks = case vecUncons ks of
    Just (h,t) ->
      snd $ V.foldl' (\(lb,res) next -> (next, res && lb < next)) (h, True) t
    Nothing    -> True
