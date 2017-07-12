module Properties.Store.File (tests) where

import Test.Framework                       (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck

import Data.Int
import Data.Proxy
import Data.Word

import Data.BTree.Impure.Structures (castNode)
import Data.BTree.Primitives
import Data.BTree.Store.File

import Properties.Impure.Structures (genLeafNode, genIndexNode)
import Properties.Utils (PageSize(..))

tests :: Test
tests = testGroup "Store.File"
    [ testProperty "binary pageMeta" prop_binary_pageMeta
    , testProperty "binary pageEmpty" prop_binary_pageEmpty
    , testProperty "binary pageNode leaf" prop_binary_pageNode_leaf
    , testProperty "binary pageNode idx" prop_binary_pageNode_idx
    ]

prop_binary_pageMeta :: Word64 -> PageSize -> Bool
prop_binary_pageMeta pc (PageSize ps)
    | Just bs <- encodeAndPad ps (PageMeta (PageCount pc))
    = case decode getMetaPage bs of
        PageMeta pc' -> pc' == PageCount pc
        _            -> False
    | otherwise = False -- should always work

prop_binary_pageEmpty :: PageSize -> Bool
prop_binary_pageEmpty (PageSize ps)
    | Just bs <- encodeAndPad ps PageEmpty
    = case decode getEmptyPage bs of
        PageEmpty -> True
        _         -> False
    | otherwise = False -- should always work

prop_binary_pageNode_leaf :: PageSize -> Property
prop_binary_pageNode_leaf (PageSize ps) = forAll genLeafNode $ \leaf ->
    case encodeAndPad ps (PageNode zeroHeight leaf) of
        Nothing -> True -- too big, skip
        Just bs -> case decode (getPageNode zeroHeight key val) bs of
            PageNode h n -> maybe False (== leaf) $ castNode h zeroHeight n
            _            -> False
 where
   key = Proxy :: Proxy Int64
   val = Proxy :: Proxy Bool

prop_binary_pageNode_idx :: PageSize -> Property
prop_binary_pageNode_idx (PageSize ps) = forAll genIndexNode $ \(srcHgt, idx) ->
    case encodeAndPad ps (PageNode srcHgt idx) of
        Nothing -> True -- too big, skip
        Just bs -> case decode (getPageNode srcHgt key val) bs of
            PageNode h n -> maybe False (== idx) $ castNode h srcHgt n
            _            -> False
 where
   key = Proxy :: Proxy Int64
   val = Proxy :: Proxy Bool
