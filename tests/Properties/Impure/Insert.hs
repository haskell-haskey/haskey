module Properties.Impure.Insert where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)

import Control.Monad ((>=>))

import Data.Int
import Data.Word (Word8)
import qualified Data.Map as M

import Data.BTree.Alloc.Debug
import Data.BTree.Impure.Insert
import qualified Data.BTree.Impure as Tree

tests :: Test
tests = testGroup "Impure.Insert"
    [ testProperty "insertTreeMany" prop_insertTreeMany
    , testProperty "insertOverflows" prop_insertOverflows
    ]

prop_insertTreeMany :: [(Int64, Integer)] -> [(Int64, Integer)] -> Bool
prop_insertTreeMany xs ys = ty1 == ty2
  where
    tx  = insertAll xs Tree.empty

    ty1 = evalDebug emptyPages $
              tx
              >>= insertAll ys
              >>= Tree.toList

    ty2 = evalDebug emptyPages $
              tx
              >>= insertTreeMany (M.fromList ys)
              >>= Tree.toList

    insertAll kvs = foldl (>=>) return (map (uncurry insertTree) kvs)

prop_insertOverflows :: M.Map Int64 [Word8] -> Bool
prop_insertOverflows kvs
    | v <- evalDebug emptyPages $
        insertTreeMany kvs Tree.empty
        >>= Tree.toList
    = v == M.toList kvs
