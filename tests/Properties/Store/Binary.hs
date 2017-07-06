module Properties.Store.Binary (tests) where

import Test.Framework                       (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck

import Data.Int
import Data.Proxy

import Data.BTree.Primitives
import Data.BTree.Store.Binary

import Properties.Primitives (genLeafNode, genIndexNode)

tests :: Test
tests = testGroup "Store.Binary"
    [ testProperty "binary pageEmpty" prop_binary_pageEmpty
    , testProperty "binary pageNode leaf" prop_binary_pageNode_leaf
    , testProperty "binary pageNode idx" prop_binary_pageNode_idx
    ]

prop_binary_pageEmpty :: Bool
prop_binary_pageEmpty = case decode getEmptyPage (encode PageEmpty) of
    PageEmpty -> True
    _         -> False

prop_binary_pageNode_leaf :: Property
prop_binary_pageNode_leaf = forAll genLeafNode $ \leaf ->
    case decode (getPageNode zeroHeight key val) (encode (PageNode zeroHeight leaf)) of
        PageNode h n -> maybe False (== leaf) $ castNode h zeroHeight n
        _            -> False
 where
   key = Proxy :: Proxy Int64
   val = Proxy :: Proxy Bool

prop_binary_pageNode_idx :: Property
prop_binary_pageNode_idx = forAll genIndexNode $ \(srcHgt, idx) ->
    case decode (getPageNode srcHgt key val) (encode (PageNode srcHgt idx)) of
        PageNode h n -> maybe False (== idx) $ castNode h srcHgt n
        _            -> False
 where
   key = Proxy :: Proxy Int64
   val = Proxy :: Proxy Bool
