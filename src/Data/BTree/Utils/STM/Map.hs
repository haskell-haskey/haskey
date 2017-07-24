module Data.BTree.Utils.STM.Map where

import Control.Concurrent.STM (STM)

import qualified ListT as L

import STMContainers.Map (Map)
import qualified STMContainers.Map as M

lookupMin :: Map k v -> STM (Maybe (k, v))
lookupMin = L.head . M.stream

lookupMinKey :: Map k v -> STM (Maybe k)
lookupMinKey = ((fst <$>) <$>) . lookupMin
