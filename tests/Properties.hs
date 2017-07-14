{-# LANGUAGE ScopedTypeVariables #-}

module Main (main) where

import Test.Framework (Test, defaultMain)

import qualified Properties.Alloc.Append
import qualified Properties.Impure.Fold
import qualified Properties.Impure.Insert
import qualified Properties.Impure.Structures
import qualified Properties.Primitives.Height
import qualified Properties.Primitives.Ids
import qualified Properties.Primitives.Index
import qualified Properties.Primitives.Leaf
import qualified Properties.Pure
import qualified Properties.Store.Binary
import qualified Properties.Store.File

--------------------------------------------------------------------------------

tests :: [Test]
tests =
    [ Properties.Alloc.Append.tests
    , Properties.Impure.Fold.tests
    , Properties.Impure.Insert.tests
    , Properties.Impure.Structures.tests
    , Properties.Primitives.Height.tests
    , Properties.Primitives.Ids.tests
    , Properties.Primitives.Index.tests
    , Properties.Primitives.Leaf.tests
    , Properties.Pure.tests
    , Properties.Store.Binary.tests
    , Properties.Store.File.tests
    ]

main :: IO ()
main = defaultMain tests

--------------------------------------------------------------------------------
