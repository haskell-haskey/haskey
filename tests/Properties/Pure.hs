{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
module Properties.Pure where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck

import Data.BTree.Primitives.Index
import Data.BTree.Primitives.Key
import Data.BTree.Pure
import qualified Data.BTree.Pure as Tree

import Control.Applicative ((<$>))

import Data.Function (on)
import Data.Int
import Data.List (nubBy)
import Data.Monoid (Sum(..))
import qualified Data.Foldable as F
import qualified Data.Map as M

tests :: Test
tests = testGroup "Pure"
    [ testProperty "foldable" prop_foldable
    , testProperty "validTree fromList" prop_validTree_fromList
    , testProperty "foldableToList fromList" prop_foldableToList_fromList
    , testProperty "toList fromList" prop_toList_fromList
    , testProperty "insertMany" prop_insertMany
    , testProperty "insert insertMany" prop_insert_insertMany
    , testProperty "lookup insert" prop_lookup_insert
    ]

instance (Key k, Arbitrary k, Arbitrary v) => Arbitrary (Tree.Tree k v) where
    arbitrary = Tree.fromList testSetup <$> arbitrary
    shrink = map (Tree.fromList testSetup) . shrink . Tree.toList

prop_foldable :: [(Int64, Int)] -> Bool
prop_foldable xs = F.foldMap snd xs' == F.foldMap id (Tree.fromList testSetup xs')
  where xs' = nubByFstEq . map(\x -> (fst x, Sum $ snd x)) $ xs

prop_validTree_fromList :: [(Int64, Int)] -> Bool
prop_validTree_fromList xs = validTree (Tree.fromList testSetup xs)

prop_foldableToList_fromList :: [(Int64, Int)] -> Bool
prop_foldableToList_fromList xs =
    F.toList (Tree.fromList testSetup xs) ==
    F.toList (M.fromList xs)

prop_toList_fromList :: [(Int64, Int)] -> Bool
prop_toList_fromList xs =
    Tree.toList (Tree.fromList testSetup xs) ==
    M.toList    (M.fromList xs)

prop_insertMany :: [(Int64, Int)] -> [(Int64, Int)] -> Bool
prop_insertMany xs ys
    | isValid <- validTree txy
    , equiv   <- Tree.toList txy == M.toList mxy
    = isValid && equiv
  where
    (mx, my) = (M.fromList xs, M.fromList ys)
    mxy = M.union mx my
    ty = Tree.fromList testSetup ys
    txy = Tree.insertMany mx ty

prop_insert_insertMany :: M.Map Int64 Int -> Tree.Tree Int64 Int -> Bool
prop_insert_insertMany kvs t =
    Tree.toList (Tree.insertMany kvs t) ==
    Tree.toList (foldl (flip $ uncurry Tree.insert) t (M.toList kvs))

prop_lookup_insert :: Int64 -> Int -> Tree.Tree Int64 Int -> Bool
prop_lookup_insert k v t = Tree.lookup k (Tree.insert k v t) == Just v

nubByFstEq :: Eq a => [(a, b)] -> [(a, b)]
nubByFstEq = nubBy ((==) `on` fst)

-- | Check whether a given tree is valid.
validTree :: Ord key => Tree key val -> Bool
validTree (Tree _ Nothing) = True
validTree (Tree setup (Just (Leaf items))) = M.size items <= maxLeafItems setup
validTree (Tree setup (Just (Idx idx))) =
    validIndexSize 1 (maxIdxKeys setup) idx && F.all (validNode setup) idx

-- | Check whether a (non-root) node is valid.
validNode :: Ord key => TreeSetup -> Node height key val -> Bool
validNode setup = \case
    Leaf items -> M.size items >= minLeafItems setup &&
                  M.size items <= maxLeafItems setup
    Idx idx    -> validIndexSize (minIdxKeys setup) (maxIdxKeys setup) idx &&
                  F.all (validNode setup) idx

testSetup :: TreeSetup
testSetup = twoThreeSetup
