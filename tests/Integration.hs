{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
module Main (main) where

import Test.Framework (Test, defaultMain)

import qualified Integration.WriteOpenRead.Append
import qualified Integration.WriteOpenRead.Concurrent
import qualified Integration.WriteOpenRead.PageReuse

tests :: [Test]
tests =
    [ Integration.WriteOpenRead.Append.tests
    , Integration.WriteOpenRead.Concurrent.tests
    , Integration.WriteOpenRead.PageReuse.tests
    ]

main :: IO ()
main = defaultMain tests
