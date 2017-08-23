{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Properties.Store.Page where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.HUnit (testCase)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.HUnit hiding (Test, Node)
import Test.QuickCheck

import Control.Applicative ((<$>))

import Data.Int
import Data.List (nub)
import Data.Monoid ((<>))
import Data.Proxy
import qualified Data.Binary as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map as M
import qualified Data.Vector as V

import Data.BTree.Impure.Structures
import Data.BTree.Primitives

import Database.Haskey.Store.Page

--------------------------------------------------------------------------------

tests :: Test
tests = testGroup "Store.Page"
    [ testProperty "binary pageType" prop_binary_pageType
    , testProperty "binary emptyPage" prop_binary_emptyPage
    , testProperty "binary nodePage leaf" prop_binary_leafNodePage
    , testProperty "binary nodePage idx" prop_binary_indexNodePage
    , testCase "zero checksum length" case_zero_checksum_length
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

case_zero_checksum_length :: Assertion
case_zero_checksum_length = do
    assertEqual "zero checksum should prepend 8 bytes" 8 $
        BL.length withZero' - BL.length without'
    assertEqual "zero checksum length should equal regular checksum lenth"
        (BL.length withZero')
        (BL.length with')
  where
    withZero' = encodeZeroChecksum pg
    without'  = encodeNoChecksum pg
    with'     = encode pg

    pg = LeafNodePage zeroHeight (Leaf M.empty :: Node 'Z Int64 Int64)

decode' :: SGet t -> BL.ByteString -> Either String (Page t)
decode' x = decode x . BL.toStrict

--------------------------------------------------------------------------------

genIndexNode :: Gen (Height ('S h), Node ('S h) Int64 Bool)
genIndexNode = do
    h <- genNonZeroHeight
    n <- Idx <$> arbitrary
    return (h, n)

genLeafNode :: Gen (Node 'Z Int64 Bool)
genLeafNode = Leaf <$> arbitrary

instance Arbitrary v => Arbitrary (LeafValue v) where
    arbitrary = oneof [RawValue <$> arbitrary, OverflowValue <$> arbitrary]

instance Arbitrary TxId where
    arbitrary = TxId <$> arbitrary

deriving instance Arbitrary (Height h)

genNonZeroHeight :: Gen (Height h)
genNonZeroHeight = suchThat arbitrary $ \h -> case viewHeight h of
    UZero   -> False
    USucc _ -> True

instance (Key k, Arbitrary k, Arbitrary v) => Arbitrary (Index k v) where
  arbitrary = do
      keys <- V.fromList . nub <$> orderedList
      vals <- V.fromList <$> vector (V.length keys + 1)
      return (Index keys vals)
  shrink (Index keys vals) =
      [ Index newKeys newVals
      | k <- [0..V.length keys - 1]
      , let (preKeys,sufKeys) = V.splitAt k keys
            newKeys           = preKeys <> V.drop 1 sufKeys
            (preVals,sufVals) = V.splitAt k vals
            newVals           = preVals <> V.drop 1 sufVals
      ]

deriving instance Arbitrary (NodeId height key val)
