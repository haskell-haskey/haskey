{-# OPTIONS_GHC -fno-warn-orphans #-}
module Properties.Alloc.Append (tests) where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck

import Control.Applicative ((<$>), (<*>))

import Data.BTree.Alloc.Append
import Data.BTree.Primitives.Key

import Data.Int

import Properties.Primitives ()     -- Arbitrary instance of Tree
import Properties.Primitives.Ids () -- Arbitrary instance of TxId, PageId
import Properties.Utils (testBinary)

instance (Key k, Arbitrary k, Arbitrary v) => Arbitrary (AppendMeta k v) where
    arbitrary = AppendMeta <$> arbitrary <*> arbitrary <*> arbitrary

tests :: Test
tests = testGroup "Alloc.Append"
    [ testProperty "binary appendMeta" test_binary_appendMeta
    ]

test_binary_appendMeta :: AppendMeta Int64 Bool -> Bool
test_binary_appendMeta = testBinary

