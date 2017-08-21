{-# LANGUAGE RecordWildCards #-}
module Properties.Primitives.Leaf (tests) where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)

import Data.BTree.Primitives.Ids
import Data.BTree.Primitives.Index
import Data.BTree.Primitives.Leaf
import Data.BTree.Pure.Setup

import Data.Int
import Data.List.Ordered (isSortedBy)
import qualified Data.Binary as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map as M
import qualified Data.Vector as V

import Properties.Primitives.Ids () -- Arbitrary instance of PageSize

tests :: Test
tests = testGroup "Primitives.Leaf"
    [ testProperty "splitLeafManyPred" prop_splitLeafManyPred
    , testProperty "splitLeafMany" prop_splitLeafMany
    ]

prop_splitLeafManyPred :: PageSize -> M.Map Int64 Int -> Bool
prop_splitLeafManyPred (PageSize pageSize) m
    | M.null m
    = True
    | Just (Index vkeys vitems) <- splitLeafManyPred pred' id m
    , (keys, maps)       <- (V.toList vkeys, V.toList vitems)
    , numKeyMapsOK <- length maps == 1 + length keys
    , predMapsOK   <- all pred' maps && all ((>= 1) . M.size) maps
    , keysMaxOK    <- all (\(key, m') -> fst (M.findMax m') <  key) $ zip keys maps
    , keysMinOK    <- all (\(key, m') -> fst (M.findMin m') >= key) $ zip keys (tail maps)
    , keysOrderOK  <- isSortedBy (<) keys
    , joinedMapsOK <- M.unions maps == m
    = numKeyMapsOK && predMapsOK && keysMaxOK && keysMinOK && keysOrderOK && joinedMapsOK
    | otherwise
    = False
  where
    pred' m' = BL.length (B.encode m') <= fromIntegral pageSize

prop_splitLeafMany  :: M.Map Int64 Int -> Bool
prop_splitLeafMany m
    | M.size m <= maxLeafItems = True
    | Index vkeys vitems <- splitLeafMany maxLeafItems id m
    , (keys, maps)       <- (V.toList vkeys, V.toList vitems)
    , numKeyMapsOK <- length maps == 1 + length keys
    , sizeMapsOK   <- all (\m' -> M.size m' >= minLeafItems && M.size m' <= maxLeafItems) maps
    , keysMaxOK    <- all (\(key, m') -> fst (M.findMax m') <  key) $ zip keys maps
    , keysMinOK    <- all (\(key, m') -> fst (M.findMin m') >= key) $ zip keys (tail maps)
    , keysOrderOK  <- isSortedBy (<) keys
    , joinedMapsOK <- M.unions maps == m
    = numKeyMapsOK && sizeMapsOK && keysMaxOK && keysMinOK && keysOrderOK && joinedMapsOK
  where
    TreeSetup{..} = twoThreeSetup

