
module Data.BTree.Internal where

import           Data.List (inits)
import           Data.Map (Map)
import qualified Data.Map as M
import           Data.Maybe (listToMaybe)
import           Data.Vector (Vector)
import qualified Data.Vector as V

--------------------------------------------------------------------------------

mapSplitAt :: Eq k => Int -> Map k v -> (Map k v, Map k v)
mapSplitAt i m
    | (l,r) <- splitAt i (M.toList m)
    = (M.fromAscList l, M.fromAscList r)

safeLast :: [a] -> Maybe a
safeLast = listToMaybe . reverse

mapInits :: Ord k => Map k v -> [Map k v]
mapInits = map M.fromList . inits . M.toList

--------------------------------------------------------------------------------

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

--------------------------------------------------------------------------------
