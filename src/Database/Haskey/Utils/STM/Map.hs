{-# LANGUAGE CPP #-}
{-# LANGUAGE ConstraintKinds #-}
module Database.Haskey.Utils.STM.Map where

import Control.Applicative ((<$>))
import Control.Concurrent.STM (STM)

import qualified Focus as F
import qualified ListT as L

#if MIN_VERSION_stm_containers(1,1,0)
import StmContainers.Map (Map)
import qualified StmContainers.Map as M
import Data.Hashable
type Key key = (Eq key, Hashable key)
#else
import STMContainers.Map (Map, Key)
import qualified STMContainers.Map as M
#endif

lookupMin :: Map k v -> STM (Maybe (k, v))
#if MIN_VERSION_stm_containers(1,1,0)
lookupMin = L.head . M.listT
#else
lookupMin = L.head . M.stream
#endif

lookupMinKey :: Map k v -> STM (Maybe k)
lookupMinKey = ((fst <$>) <$>) . lookupMin

alter :: Key k => k -> (Maybe v -> Maybe v) -> Map k v -> STM ()
alter k f = M.focus (F.alterM (return . f)) k
