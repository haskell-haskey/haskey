{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
module Properties.Primitives.Ids (tests) where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck

import Control.Applicative ((<$>))

import Data.Int

import Data.BTree.Primitives.Ids

import Properties.Utils (testBinary)

instance Arbitrary PageSize where
    arbitrary = PageSize . fromIntegral <$> elements pows
      where
        -- minimum page size is 128 (fits at
        -- least two keys in an index node)
        pows = ((2 :: Int) ^) <$> ([7..12] :: [Int])

deriving instance Arbitrary (NodeId height key val)
deriving instance Arbitrary PageId
deriving instance Arbitrary TxId

prop_binary_nodeId :: NodeId h Int64 Bool -> Bool
prop_binary_nodeId = testBinary

prop_binary_pageId :: PageId -> Bool
prop_binary_pageId = testBinary

prop_binary_txId :: TxId -> Bool
prop_binary_txId = testBinary

tests :: Test
tests = testGroup "Primitives.Ids"
    [ testProperty "binary nodeId" prop_binary_nodeId
    , testProperty "binary pageId" prop_binary_pageId
    , testProperty "binary txId" prop_binary_txId
    ]
