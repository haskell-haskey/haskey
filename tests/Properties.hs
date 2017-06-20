{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Main (main) where

import Control.Applicative ((<$>))

import qualified Data.BTree.Pure as Tree
import Data.BTree.Primitives.Index
import Data.BTree.Primitives.Key
import Data.BTree.Primitives.Leaf

import Data.Int
import Data.Monoid
import Data.List (nub, nubBy)
import Data.List.Ordered (isSortedBy)
import qualified Data.Foldable as F
import qualified Data.Map as M
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

prop_splitLeafMany  :: M.Map Int64 Int -> Bool
prop_splitLeafMany m
    | M.size m <= maxLeafItems = True
    | (keys, maps) <- splitLeafMany maxLeafItems m
    , numKeyMapsOK <- length maps == 1 + length keys
    , sizeMapsOK   <- all (\m' -> M.size m' >= minLeafItems && M.size m' <= maxLeafItems) maps
    , keysMaxOK    <- all (\(key, m') -> fst (M.findMax m') <  key) $ zip keys maps
    , keysMinOK    <- all (\(key, m') -> fst (M.findMin m') >= key) $ zip keys (tail maps)
    , keysOrderOK  <- isSortedBy (<) keys
    , joinedMapsOK <- M.unions maps == m
    = numKeyMapsOK && sizeMapsOK && keysMaxOK && keysMinOK && keysOrderOK && joinedMapsOK
  where
    minLeafItems = Tree.minLeafItems
    maxLeafItems = Tree.maxLeafItems

prop_foldable :: [(Int64, Int)] -> Bool
prop_foldable xs = F.foldMap snd xs' == F.foldMap id (Tree.fromList xs')
  where xs' = nubByFstEq . map (\x -> (fst x, Sum $ snd x)) $ xs

prop_toList_fromList :: [(Int64, Int)] -> Bool
prop_toList_fromList xs = F.toList (Tree.fromList xs') == F.toList (M.fromList xs')
  where xs' = nubByFstEq xs

prop_insertRecMany :: [(Int64, Int)] -> Int -> Bool
prop_insertRecMany xs i = F.toList fromListSeparately == F.toList fromListSimul
  where
    foldrInsert = foldr (uncurry Tree.insert)

    fromListSeparately = foldrInsert (foldrInsert Tree.empty a) b
    fromListSimul      = Tree.insertMany (M.fromList b) $ foldrInsert Tree.empty a

    xs' = nubByFstEq xs
    (a, b) | null xs'  = ([], [])
           | otherwise = splitAt (i `mod` length xs') xs'

nubByFstEq :: Eq a => [(a, b)] -> [(a, b)]
nubByFstEq = nubBy (\x y -> fst x == fst y)

tests :: [Test]
tests =
    [ testGroup "Index"
        [ testProperty "valid arbitrary" prop_valid_arbitrary
        , testProperty "valid singletonIndex" prop_valid_singletonIndex
        , testProperty "mergeIndex splitIndex" prop_mergeIndex_splitIndex
        , testProperty "fromSingletonIndex singletonIndex"
            prop_fromSingletonIndex_singletonIndex
        , testProperty "distribute" prop_distribute
        , testProperty "splitIndexMany" prop_splitIndexMany
        ]
    , testGroup "Leaf"
        [ testProperty "splitLeafMany" prop_splitLeafMany
        ]
    , testGroup "Tree"
        [ testProperty "foldable" prop_foldable
        , testProperty "toList fromList" prop_toList_fromList
        , testProperty "insertRecMany" prop_insertRecMany
        ]
    ]

main :: IO ()
main = defaultMain tests

--------------------------------------------------------------------------------
