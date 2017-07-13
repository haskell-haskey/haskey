module Properties.Fold (tests) where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)

import Data.BTree.Alloc.Append
import Data.BTree.Insert
import Data.BTree.Primitives.Key
import Data.BTree.Primitives.Value
import Data.BTree.Store.Binary
import qualified Data.BTree.Fold as Tree

import Control.Monad ((>=>))

import Data.Int
import qualified Data.Map as M

tests :: Test
tests = testGroup "Fold"
    [ testProperty "foldable toList fromList" prop_foldable_toList_fromList
    ]

prop_foldable_toList_fromList :: [(Int64, Integer)] -> Bool
prop_foldable_toList_fromList kvs
    | Just l <- evalStore (createAppendDb "Main"
                           >>= insertAll kvs
                           >>= readTransact Tree.toList)
    = l == M.toList (M.fromList kvs)
    | otherwise = False

insertAll :: (AppendMetaStoreM hnd m, Key key, Value val)
         => [(key, val)]
         -> AppendDb hnd key val
         -> m (AppendDb hnd key val)
insertAll kvs = transact (foldl (>=>) return (map (uncurry insertTree) kvs))
