{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
module Main (main) where

import Test.Framework (Test, defaultMain)

import qualified Integration.WriteOpenRead

tests :: [Test]
tests =
    [ Integration.WriteOpenRead.tests
    ]

main :: IO ()
main = defaultMain tests
