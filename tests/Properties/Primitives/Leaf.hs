module Properties.Primitives.Leaf (tests) where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)

import Data.BTree.Primitives.Leaf
import qualified Data.BTree.TwoThree as Tree

import Data.Int
import Data.List.Ordered (isSortedBy)
import qualified Data.Map as M

tests :: Test
tests = testGroup "Primitives.Leaf"
    [ testProperty "splitLeafMany" prop_splitLeafMany
    ]

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

