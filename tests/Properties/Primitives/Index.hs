{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE RecordWildCards #-}
module Properties.Primitives.Index (tests) where

import Test.Framework                       (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck

import Control.Applicative ((<$>))

import Data.Int
import Data.List (nub)
import Data.List.Ordered (isSortedBy)
import Data.Maybe (isNothing)
import Data.Monoid ((<>))
import qualified Data.Binary as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Foldable as F
import qualified Data.Map as M
import qualified Data.Vector as V

import Data.BTree.Primitives.Ids
import Data.BTree.Primitives.Index
import Data.BTree.Primitives.Key
import Data.BTree.Pure.Setup

import Properties.Primitives.Ids () -- Arbitrary instance of PageSize

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
tests = testGroup "Primitives.Index"
    [ testProperty "binary" prop_binary
    , testProperty "validIndex arbitrary" prop_validIndex_arbitrary
    , testProperty "validIndex singletonIndex" prop_validIndex_singletonIndex
    , testProperty "mergeIndex splitIndexAt" prop_mergeIndex_splitIndexAt
    , testProperty "fromSingletonIndex singletonIndex"
        prop_fromSingletonIndex_singletonIndex
    , testProperty "distribute" prop_distribute
    , testProperty "extendedIndex" prop_extendedIndex
    , testProperty "extendIndexPred" prop_extendIndexPred
    , testProperty "bindIndex_extendedIndex" prop_bindIndex_extendedIndex
    ]

prop_binary :: Index Int64 Bool -> Bool
prop_binary x = x == B.decode (B.encode x)

prop_validIndex_arbitrary :: Index Int64 Bool -> Bool
prop_validIndex_arbitrary = validIndex

prop_validIndex_singletonIndex :: Int64 -> Bool
prop_validIndex_singletonIndex i =
    validIndex (singletonIndex i :: Index Int64 Int64)

prop_mergeIndex_splitIndexAt :: Property
prop_mergeIndex_splitIndexAt =
    forAll (arbitrary `suchThat` (isNothing . fromSingletonIndex)) $ \ix ->
      and [ mergeIndex left middle right == (ix :: Index Int64 Bool)
          | k <- [0..indexNumKeys ix - 1]
          , let (left, middle, right) = splitIndexAt k ix
          ]

prop_fromSingletonIndex_singletonIndex :: Int64 -> Bool
prop_fromSingletonIndex_singletonIndex i =
    fromSingletonIndex (singletonIndex i) == Just i

prop_distribute :: M.Map Int64 Int -> Index Int64 Int -> Bool
prop_distribute kvs idx
    | idx'@(Index keys vs) <- distribute kvs idx
    , x <- V.all pred1 $ V.zip keys (V.init $ V.map fst vs)
    , y <- V.all pred2 $ V.zip keys (V.tail $ V.map fst vs)
    , z <- M.unions (V.toList $ V.map fst vs) == kvs
    , u <- validIndex idx'
    = x && y && z && u
  where
    pred1 (key, sub) = M.null sub || fst (M.findMax sub) <  key
    pred2 (key, sub) = M.null sub || fst (M.findMin sub) >= key

prop_extendedIndex :: Index Int64 Int -> Bool
prop_extendedIndex idx
    | Index keys idxs <- extendedIndex maxIdxKeys id idx
    , numKeyIdxsOK    <- V.length idxs == 1 + V.length keys
    , validIdxs       <- V.all validIndex idxs
    , keysMaxOK       <- V.all (\(key, Index keys' _) -> V.last keys' < key) $ V.zip keys idxs
    , keysMinOK       <- V.all (\(key, Index keys' _) -> V.head keys' > key) $ V.zip keys (V.tail idxs)
    , keysOrderOK     <- isSortedBy (<) (V.toList keys)
    , joinedNodesOK   <- concatMap F.toList (V.toList idxs) == F.toList idx
    = numKeyIdxsOK && validIdxs && keysMaxOK && keysMinOK && keysOrderOK && joinedNodesOK
  where
    TreeSetup{..} = twoThreeSetup

prop_extendIndexPred :: PageSize -> Index Int64 Int -> Bool
prop_extendIndexPred (PageSize pageSize) idx
    | indexNumVals idx <= 2
    = True
    | Just (Index keys idxs) <- extendIndexPred pred' id idx
    , numKeyIdxsOK    <- V.length idxs == 1 + V.length keys
    , validIdxs       <- V.all validIndex idxs
    , keysMaxOK       <- V.all (\(key, Index keys' _) -> V.last keys' < key) $ V.zip keys idxs
    , keysOrderOK     <- isSortedBy (<) (V.toList keys)
    , joinedNodesOK   <- concatMap F.toList (V.toList idxs) == F.toList idx
    = numKeyIdxsOK && validIdxs && keysMaxOK && keysOrderOK && joinedNodesOK
    | otherwise
    = False
  where
    pred' m' = BL.length (B.encode m') <= fromIntegral pageSize

prop_bindIndex_extendedIndex :: Int -> Index Int64 Int -> Bool
prop_bindIndex_extendedIndex n idx =
    bindIndex (extendedIndex (abs n + 1) id idx) id == idx
