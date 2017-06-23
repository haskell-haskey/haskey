{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
module Properties.Primitives (tests) where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck

import Data.BTree.Primitives

import Data.Int
import qualified Data.Binary as B

import Properties.Primitives.Index () -- Arbitrary instance of Index
import Properties.Primitives.Ids ()   -- Arbitrary instance of NodeId

tests :: Test
tests = testGroup "Primitives.Ids"
    [ testProperty "binary leafNode" prop_binary_leafNode
    , testProperty "binary indexNode" prop_binary_indexNode
    ]

instance (Key k, Arbitrary k, Arbitrary v) => Arbitrary (Node 'Z k v) where
    arbitrary = Leaf <$> arbitrary

instance (Key k, Arbitrary k, Arbitrary v) => Arbitrary (Node ('S height) k v) where
    arbitrary = Idx <$> arbitrary

prop_binary_leafNode :: Node 'Z Int64 Bool -> Bool
prop_binary_leafNode x = B.decode (B.encode x) == x

prop_binary_indexNode :: Node ('S height) Int64 Bool -> Bool
prop_binary_indexNode x = B.decode (B.encode x) == x
