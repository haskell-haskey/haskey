{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
module Properties.Primitives.Height (tests, genNonZeroHeight) where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck

import Data.BTree.Primitives.Height

import Properties.Utils (testBinary)

deriving instance Arbitrary (Height h)

genNonZeroHeight :: Gen (Height h)
genNonZeroHeight = suchThat arbitrary $ \h -> case viewHeight h of
    UZero   -> False
    USucc _ -> True

tests :: Test
tests = testGroup "Primitives.Height"
    [ testProperty "binary" prop_binary
    ]

prop_binary :: Height h -> Bool
prop_binary = testBinary
