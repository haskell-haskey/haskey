module Properties.Insert (tests) where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)

import Data.BTree.Alloc.Append
import Data.BTree.Insert
import Data.BTree.Store.Debug
import qualified Data.BTree.Fold as Tree

import Control.Monad ((>=>))

import Data.Int
import Data.Maybe (isJust)
import qualified Data.Map as M

tests :: Test
tests = testGroup "Insert"
    [ testProperty "insertTreeMany" prop_insertTreeMany
    ]

prop_insertTreeMany :: [(Int64, Integer)] -> [(Int64, Integer)] -> Bool
prop_insertTreeMany xs ys = ty1 == ty2 && isJust ty1
  where
    tx  = createAppendDb "Main" >>= insertAll xs
    ty1 = evalStore $ tx >>= insertAll ys
                         >>= readTransact Tree.toList
    ty2 = evalStore $ tx >>= transact (insertTreeMany $ M.fromList ys)
                         >>= readTransact Tree.toList
    insertAll kvs = transact (foldl (>=>) return (map (uncurry insertTree) kvs))
