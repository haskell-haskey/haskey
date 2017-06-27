{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
module Properties.Primitives (tests, treeEqShape) where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck

import Control.Applicative ((<$>), (<*>))

import Data.BTree.Primitives

import Data.Int
import Data.Typeable
import qualified Data.Binary as B

import Properties.Primitives.Height () -- Arbitrary instance of Height
import Properties.Primitives.Index ()  -- Arbitrary instance of Index
import Properties.Primitives.Ids ()    -- Arbitrary instance of NodeId

import Properties.Utils (testBinary)

tests :: Test
tests = testGroup "Primitives.Ids"
    [ testProperty "binary leafNode" prop_binary_leafNode
    , testProperty "binary indexNode" prop_binary_indexNode
    , testProperty "binary tree" prop_binary_tree
    ]

instance (Key k, Arbitrary k, Arbitrary v) => Arbitrary (Node 'Z k v) where
    arbitrary = Leaf <$> arbitrary

instance (Key k, Arbitrary k) => Arbitrary (Node ('S height) k v) where
    arbitrary = Idx <$> arbitrary

instance Arbitrary (Tree k v) where
    arbitrary = Tree <$> arbitrary <*> arbitrary

prop_binary_leafNode :: Node 'Z Int64 Bool -> Bool
prop_binary_leafNode = testBinary

prop_binary_indexNode :: Node ('S height) Int64 Bool -> Bool
prop_binary_indexNode = testBinary

prop_binary_tree :: Tree Int64 Bool -> Bool
prop_binary_tree t = B.decode (B.encode t) `treeEqShape` t

--------------------------------------------------------------------------------
--
{-| Compare the shape of a 'Tree' structure -}
treeEqShape :: (Typeable key, Typeable val)
            => Tree key val
            -> Tree key val
            -> Bool
Tree hx Nothing   `treeEqShape` Tree hy Nothing   = fromHeight hx == fromHeight hy
Tree hx (Just rx) `treeEqShape` Tree hy (Just ry) =
    maybe False (== ry) $ castNode hx hy rx
Tree _ _          `treeEqShape` Tree _ _          = False
