module Properties.Impure.Insert where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)

import Control.Monad ((>=>))

import Data.Int
import Data.Maybe (isJust)
import qualified Data.Map as M

import Data.BTree.Alloc.Append
import Data.BTree.Impure.Insert
import Data.BTree.Store.Binary
import qualified Data.BTree.Impure.Fold as Tree

tests :: Test
tests = testGroup "Impure.Insert"
    [ testProperty "insertTreeMany" prop_insertTreeMany
    ]

prop_insertTreeMany :: [(Int64, Integer)] -> [(Int64, Integer)] -> Bool
prop_insertTreeMany xs ys = ty1 == ty2 && isJust ty1
  where
    tx  = createAppendDb "Main" >>= insertAll xs
    ty1 = evalStore $ tx >>= insertAll ys
                         >>= transactReadOnly Tree.toList
    ty2 = evalStore $ tx >>= transact_ (insertTreeMany (M.fromList ys) >=> commit_)
                         >>= transactReadOnly Tree.toList
    insertAll kvs = transact_ $
        foldl (>=>) return (map (uncurry insertTree) kvs)
        >=> commit_
