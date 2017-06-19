{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main (main) where

import Control.Applicative ((<$>))

import Data.BTree.Primitives.Index
import Data.BTree.Primitives.Key

import Data.Int
import Data.Monoid
import Data.List (nub)
import qualified Data.Vector as V
import Test.Framework (Test, defaultMain, testGroup)
import Test.Framework.Providers.QuickCheck2
import Test.QuickCheck

default (Int64)

--------------------------------------------------------------------------------

instance (Key k, Arbitrary k, Arbitrary v) => Arbitrary (Index k v) where
  arbitrary = do
      keys <- V.fromList . nub <$> orderedList
      vals <- V.fromList <$> vector (V.length keys + 1)
      return (Index keys vals)
  shrink (Index keys vals) =
      [ Index newKeys newVals
      | k <- [0..V.length keys - 1]
      , let (preKeys,sufKeys) = V.splitAt k keys
            newKeys           = preKeys <> V.drop 1 sufKeys
            (preVals,sufVals) = V.splitAt k vals
            newVals           = preVals <> V.drop 1 sufVals
      ]

prop_valid_arbitrary :: Index Int64 Bool -> Bool
prop_valid_arbitrary = validIndex

prop_valid_singletonIndex :: Int64 -> Bool
prop_valid_singletonIndex i =
    validIndex (singletonIndex i :: Index Int64 Int64)

prop_mergeIndex_splitIndex :: Property
prop_mergeIndex_splitIndex =
    forAll (arbitrary `suchThat` (not . V.null . indexKeys)) $ \ix ->
      let (left, middle, right) = splitIndex (ix :: Index Int64 Bool)
      in  mergeIndex left middle right == ix

prop_fromSingletonIndex_singletonIndex :: Int64 -> Bool
prop_fromSingletonIndex_singletonIndex i =
    fromSingletonIndex (singletonIndex i) == Just i

tests :: [Test]
tests =
    [ testGroup "Index"
        [ testProperty "valid arbitrary" prop_valid_arbitrary
        , testProperty "valid singletonIndex" prop_valid_singletonIndex
        , testProperty "mergeIndex splitIndex" prop_mergeIndex_splitIndex
        , testProperty "fromSingletonIndex singletonIndex"
            prop_fromSingletonIndex_singletonIndex
        ]
    ]

main :: IO ()
main = defaultMain tests

--------------------------------------------------------------------------------
