module Properties.Pure where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)

import qualified Data.BTree.Pure as Tree

import Data.Int
import Data.List (nubBy)
import qualified Data.Foldable as F
import qualified Data.Map as M

tests :: Test
tests = testGroup "Pure"
    [ testProperty "foldable toList fromList" prop_foldable_toList_fromList
    , testProperty "insertRecMany" prop_insertRecMany
    ]

prop_foldable_toList_fromList :: [(Int64, Int)] -> Bool
prop_foldable_toList_fromList xs = F.toList (Tree.fromList xs') == F.toList (M.fromList xs')
  where xs' = nubByFstEq xs

prop_insertRecMany :: [(Int64, Int)] -> Int -> Bool
prop_insertRecMany xs i
    | isValid   <- Tree.validTree fromListSimul
    , equiv     <- F.toList fromListSeparately == F.toList fromListSimul
    = isValid && equiv
  where
    foldrInsert = foldr (uncurry Tree.insert)

    fromListSeparately = foldrInsert (foldrInsert Tree.empty a) b
    fromListSimul      = Tree.insertMany (M.fromList b) $ foldrInsert Tree.empty a

    xs' = nubByFstEq xs
    (a, b) | null xs'  = ([], [])
           | otherwise = splitAt (i `mod` length xs') xs'

nubByFstEq :: Eq a => [(a, b)] -> [(a, b)]
nubByFstEq = nubBy (\x y -> fst x == fst y)
