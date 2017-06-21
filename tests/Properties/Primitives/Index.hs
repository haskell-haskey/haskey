{-# OPTIONS_GHC -fno-warn-orphans #-}
module Properties.Primitives.Index where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck

import Data.BTree.Primitives.Index
import Data.BTree.Primitives.Key
import qualified Data.BTree.TwoThree as Tree

import Control.Applicative ((<$>))

import Data.Int
import Data.List (nub)
import Data.List.Ordered (isSortedBy)
import Data.Monoid ((<>))
import qualified Data.Map as M
import qualified Data.Vector as V

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

tests :: Test
tests = testGroup "Index"
    [ testProperty "validIndex arbitrary" prop_validIndex_arbitrary
    , testProperty "validIndex singletonIndex" prop_validIndex_singletonIndex
    , testProperty "mergeIndex splitIndex" prop_mergeIndex_splitIndex
    , testProperty "fromSingletonIndex singletonIndex"
        prop_fromSingletonIndex_singletonIndex
    , testProperty "distribute" prop_distribute
    , testProperty "splitIndexMany" prop_splitIndexMany
    ]

prop_validIndex_arbitrary :: Index Int64 Bool -> Bool
prop_validIndex_arbitrary = validIndex

prop_validIndex_singletonIndex :: Int64 -> Bool
prop_validIndex_singletonIndex i =
    validIndex (singletonIndex i :: Index Int64 Int64)

prop_mergeIndex_splitIndex :: Property
prop_mergeIndex_splitIndex =
    forAll (arbitrary `suchThat` (not . V.null . indexKeys)) $ \ix ->
      let (left, middle, right) = splitIndex (ix :: Index Int64 Bool)
      in  mergeIndex left middle right == ix

prop_fromSingletonIndex_singletonIndex :: Int64 -> Bool
prop_fromSingletonIndex_singletonIndex i =
    fromSingletonIndex (singletonIndex i) == Just i

prop_distribute :: M.Map Int64 Int -> Index Int64 Int -> Bool
prop_distribute kvs idx
    | idx'@Index { indexKeys = keys, indexNodes = vs } <- distribute kvs idx
    , x <- V.all pred1 $ V.zip keys (V.init $ V.map fst vs)
    , y <- V.all pred2 $ V.zip keys (V.tail $ V.map fst vs)
    , z <- M.unions (V.toList $ V.map fst vs) == kvs
    , u <- validIndex idx'
    = x && y && z && u
  where
    pred1 (key, sub) = M.null sub || fst (M.findMax sub) <  key
    pred2 (key, sub) = M.null sub || fst (M.findMin sub) >= key

prop_splitIndexMany :: Index Int64 Int -> Bool
prop_splitIndexMany idx
    | V.length (indexKeys idx) <= maxIdxKeys = True
    | (keys, idxs)  <- splitIndexMany maxIdxKeys idx
    , numKeyIdxsOK  <- length idxs == 1 + length keys
    , validIdxs     <- all validIndex idxs
    , keysMaxOK     <- all (\(key, idx') -> V.last (indexKeys idx') < key) $ zip keys idxs
    , keysMinOK     <- all (\(key, idx') -> V.head (indexKeys idx') > key) $ zip keys (tail idxs)
    , keysOrderOK   <- isSortedBy (<) keys
    , joinedNodesOK <- V.concat (map indexNodes idxs) == indexNodes idx
    = numKeyIdxsOK && validIdxs && keysMaxOK && keysMinOK && keysOrderOK && joinedNodesOK
  where
    maxIdxKeys = Tree.maxFanout - 1
