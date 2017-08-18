{-# LANGUAGE GADTs #-}
module Properties.Store.Page where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck

import Data.Int
import Data.Proxy
import qualified Data.Binary as B
import qualified Data.ByteString.Lazy as BL

import Data.BTree.Impure.Structures (castNode)
import Data.BTree.Primitives
import Data.BTree.Store.Page

import Properties.Impure.Structures (genLeafNode, genIndexNode)

tests :: Test
tests = testGroup "Store.Page"
    [ testProperty "binary pageType" prop_binary_pageType
    , testProperty "binary emptyPage" prop_binary_emptyPage
    , testProperty "binary nodePage leaf" prop_binary_leafNodePage
    , testProperty "binary nodePage idx" prop_binary_indexNodePage
    ]

prop_binary_pageType :: Property
prop_binary_pageType = forAll types $ \t ->
    let bs = B.encode t in BL.length bs == 1 && B.decode bs == t
  where
    types = elements [TypeEmpty,
                      TypeConcurrentMeta,
                      TypeOverflow,
                      TypeLeafNode,
                      TypeIndexNode]

prop_binary_emptyPage :: Bool
prop_binary_emptyPage = case decode' emptyPage (encode EmptyPage) of
    Right EmptyPage -> True
    Left _          -> False

prop_binary_leafNodePage :: Property
prop_binary_leafNodePage = forAll genLeafNode $ \leaf ->
    case decode' (leafNodePage zeroHeight key val)
                 (encode (LeafNodePage zeroHeight leaf)) of
        Right (LeafNodePage h n) -> maybe False (== leaf) $ castNode h zeroHeight n
        Left _                   -> False
 where
   key = Proxy :: Proxy Int64
   val = Proxy :: Proxy Bool

prop_binary_indexNodePage :: Property
prop_binary_indexNodePage = forAll genIndexNode $ \(srcHgt, idx) ->
    case decode' (indexNodePage srcHgt key val)
                 (encode (IndexNodePage srcHgt idx)) of
        Right (IndexNodePage h n) -> maybe False (== idx) $ castNode h srcHgt n
        Left _                    -> False
 where
   key = Proxy :: Proxy Int64
   val = Proxy :: Proxy Bool

decode' :: SGet t -> BL.ByteString -> Either String (Page t)
decode' x = decode x . BL.toStrict
