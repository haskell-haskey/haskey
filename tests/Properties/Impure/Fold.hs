module Properties.Impure.Fold where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)

import Control.Monad ((>=>))

import Data.Int
import qualified Data.Map as M

import Data.BTree.Alloc.Debug
import Data.BTree.Impure.Insert
import qualified Data.BTree.Impure as Tree

tests :: Test
tests = testGroup "Impure.Fold"
    [ testProperty "foldable toList fromList" prop_foldable_toList_fromList
    ]

prop_foldable_toList_fromList :: [(Int64, Integer)] -> Bool
prop_foldable_toList_fromList kvs
    | (v, _) <- runDebug emptyPages $
        foldl (>=>) return (map (uncurry insertTree) kvs) Tree.empty
         >>= Tree.toList
    = v == M.toList (M.fromList kvs)
