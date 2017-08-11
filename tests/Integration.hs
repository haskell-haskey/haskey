{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
module Main (main) where

import Test.Framework (Test, defaultMain)

import qualified Integration.ExceptionHandling.Concurrent
import qualified Integration.WriteOpenRead.Concurrent

tests :: [Test]
tests =
    [ Integration.ExceptionHandling.Concurrent.tests
    , Integration.WriteOpenRead.Concurrent.tests
    ]

main :: IO ()
main = defaultMain tests
