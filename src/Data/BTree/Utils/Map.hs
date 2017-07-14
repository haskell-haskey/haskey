module Data.BTree.Utils.Map where

import Data.List (inits)
import Data.Map (Map)
import qualified Data.Map as M

mapSplitAt :: Eq k => Int -> Map k v -> (Map k v, Map k v)
mapSplitAt i m
    | (l,r) <- splitAt i (M.toList m)
    = (M.fromAscList l, M.fromAscList r)

mapInits :: Ord k => Map k v -> [Map k v]
mapInits = map M.fromList . inits . M.toList
