{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
module Properties.Primitives.Ids (tests) where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck

import Data.BTree.Primitives.Ids

import Data.Int
import qualified Data.Binary as B

deriving instance (Arbitrary key, Arbitrary val) => Arbitrary (NodeId height key val)

prop_binary :: NodeId h Int64 Bool -> Bool
prop_binary x = B.decode (B.encode x) == x

tests :: Test
tests = testGroup "Primitives.Ids"
    [ testProperty "binary" prop_binary
    ]
