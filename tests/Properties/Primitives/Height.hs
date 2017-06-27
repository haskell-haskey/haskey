{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
module Properties.Primitives.Height (tests) where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck

import Data.BTree.Primitives.Height

import Properties.Utils (testBinary)

deriving instance Arbitrary (Height h)

tests :: Test
tests = testGroup "Primitives.Height"
    [ testProperty "binary" prop_binary
    ]

prop_binary :: Height h -> Bool
prop_binary = testBinary
