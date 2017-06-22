{-# LANGUAGE ScopedTypeVariables #-}

module Main (main) where

import Test.Framework (Test, defaultMain)

import qualified Properties.Fold
import qualified Properties.Insert
import qualified Properties.Primitives.Index
import qualified Properties.Primitives.Leaf
import qualified Properties.Pure

--------------------------------------------------------------------------------

tests :: [Test]
tests =
    [ Properties.Fold.tests
    , Properties.Insert.tests
    , Properties.Primitives.Index.tests
    , Properties.Primitives.Leaf.tests
    , Properties.Pure.tests
    ]

main :: IO ()
main = defaultMain tests

--------------------------------------------------------------------------------
