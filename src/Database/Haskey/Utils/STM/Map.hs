{-# LANGUAGE ConstraintKinds #-}
module Database.Haskey.Utils.STM.Map where

import Control.Applicative ((<$>))
import Control.Concurrent.STM (STM)

import qualified Focus as F
import qualified ListT as L

import STMContainers.Map (Map, Key)
import qualified STMContainers.Map as M

lookupMin :: Map k v -> STM (Maybe (k, v))
lookupMin = L.head . M.stream

lookupMinKey :: Map k v -> STM (Maybe k)
lookupMinKey = ((fst <$>) <$>) . lookupMin

alter :: Key k => k -> (Maybe v -> Maybe v) -> Map k v -> STM ()
alter k f = M.focus (F.alterM (return . f)) k
