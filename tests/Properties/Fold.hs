module Properties.Fold where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)

import Data.BTree.Alloc.Append
import Data.BTree.Insert
import Data.BTree.Primitives.Key
import Data.BTree.Primitives.Value
import Data.BTree.Store.Debug
import qualified Data.BTree.Fold as Tree

import Control.Monad ((>=>))
import Control.Monad.Identity

import Data.Int
import qualified Data.Foldable as F
import qualified Data.Map as M

tests :: Test
tests = testGroup "Fold"
    [ testProperty "foldable toList fromList" prop_foldable_toList_fromList
    ]

prop_foldable_toList_fromList :: [(Int64, Integer)] -> Bool
prop_foldable_toList_fromList kvs
    | Just l <- runInMemory (createAppendDb "Main"
                             >>= insertAll kvs
                             >>= readTransact Tree.toList)
    = l == F.toList (M.fromList kvs)
    | otherwise = False

runInMemory :: StoreT String Identity a -> Maybe a
runInMemory = fst . runIdentity . flip runStoreT initialStore
  where
    initialStore = M.fromList [ ("Main", M.empty) ]

insertAll :: (AppendMetaStoreM hnd m, Key key, Value val)
         => [(key, val)]
         -> AppendDb hnd key val
         -> m (AppendDb hnd key val)
insertAll kvs = transact (foldl (>=>) return (map (uncurry insertTree) kvs))
