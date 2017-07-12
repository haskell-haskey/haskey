{-# OPTIONS_GHC -fno-warn-orphans #-}
module Properties.Alloc.Append (tests) where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck

import Control.Applicative ((<$>), (<*>))

import Data.Int
import Data.Typeable
import qualified Data.Binary as B

import Data.BTree.Alloc.Append

import Properties.Impure.Structures (treeEqShape)
import Properties.Primitives.Ids () -- Arbitrary instance of TxId, PageId

instance Arbitrary (AppendMeta k v) where
    arbitrary = AppendMeta <$> arbitrary <*> arbitrary <*> arbitrary

tests :: Test
tests = testGroup "Alloc.Append"
    [ testProperty "binary appendMeta" test_binary_appendMeta
    ]

test_binary_appendMeta :: AppendMeta Int64 Bool -> Bool
test_binary_appendMeta x = B.decode (B.encode x) `appendMetaEq` x

--------------------------------------------------------------------------------

{-| Compare the the shape of the 'AppendMeta' structure -}
appendMetaEq :: (Typeable k, Typeable v)
             => AppendMeta k v
             -> AppendMeta k v
             -> Bool
x `appendMetaEq` y =
    appendMetaRevision x == appendMetaRevision y &&
    appendMetaPrevious x == appendMetaPrevious y &&
    appendMetaTree x `treeEqShape` appendMetaTree y
