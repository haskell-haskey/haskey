module Properties.Primitives.Leaf (tests) where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck

import Data.BTree.Primitives.Index
import Data.BTree.Primitives.Leaf
import qualified Data.BTree.TwoThree as Tree

import Data.Int
import Data.List.Ordered (isSortedBy)
import qualified Data.Binary as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map as M
import qualified Data.Vector as V

tests :: Test
tests = testGroup "Primitives.Leaf"
    [ testProperty "splitLeafManyPred" prop_splitLeafManyPred
    , testProperty "splitLeafMany" prop_splitLeafMany
    ]

newtype PageSize = PageSize Int64 deriving (Show)

instance Arbitrary PageSize where
    arbitrary = PageSize . fromIntegral <$> elements pows
      where pows = ((2 :: Int) ^) <$> ([5..12] :: [Int])


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
    pred' m' = BL.length (B.encode m') <= pageSize

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
    minLeafItems = Tree.minLeafItems
    maxLeafItems = Tree.maxLeafItems

