module Main (main) where

import Test.Framework (Test, defaultMain)

import qualified Properties.Store.Page

--------------------------------------------------------------------------------

tests :: [Test]
tests =
    [ Properties.Store.Page.tests
    ]

main :: IO ()
main = defaultMain tests

--------------------------------------------------------------------------------
