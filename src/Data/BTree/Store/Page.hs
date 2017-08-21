{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
-- | This module contains structures and functions to decode and encode
-- pages.
module Data.BTree.Store.Page where

import Codec.Compression.LZ4

import Control.Applicative ((<$>))
import Control.Monad.Catch

import Data.Binary (Binary(..), Put, Get)
import Data.Binary.Get (runGetOrFail)
import Data.Binary.Put (runPut)
import Data.Bits ((.&.), (.|.))
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Digest.Adler32 (adler32)
import Data.Maybe (fromMaybe)
import Data.Proxy
import Data.Typeable (Typeable)
import Data.Word (Word8, Word32)
import qualified Data.Binary as B
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL

import Numeric (showHex)

import Data.BTree.Alloc.Concurrent
import Data.BTree.Impure
import Data.BTree.Impure.Structures (putLeafNode, getLeafNode, putIndexNode, getIndexNode)
import Data.BTree.Primitives

-- | The type of a page.
data PageType = TypeEmpty
              | TypeConcurrentMeta
              | TypeOverflow
              | TypeLeafNode
              | TypeIndexNode
              deriving (Eq, Show)

data SPageType t where
    STypeEmpty           :: SPageType 'TypeEmpty
    STypeConcurrentMeta  :: SPageType 'TypeConcurrentMeta
    STypeOverflow        :: SPageType 'TypeOverflow
    STypeLeafNode        :: SPageType 'TypeLeafNode
    STypeIndexNode       :: SPageType 'TypeIndexNode

instance Binary PageType where
    put TypeEmpty          = put (0x00 :: Word8)
    put TypeConcurrentMeta = put (0x20 :: Word8)
    put TypeOverflow       = put (0x40 :: Word8)
    put TypeLeafNode       = put (0x60 :: Word8)
    put TypeIndexNode      = put (0x80 :: Word8)
    get = (get :: Get Word8) >>= \case
        0x00 -> return TypeEmpty
        0x20 -> return TypeConcurrentMeta
        0x40 -> return TypeOverflow
        0x60 -> return TypeLeafNode
        0x80 -> return TypeIndexNode
        t    -> fail $ "unknown page type: " ++ showHex t ""

-- | A decoded page, of a certain type @t@ of kind 'PageType'.
data Page (t :: PageType) where
    EmptyPage :: Page 'TypeEmpty
    ConcurrentMetaPage :: (Key k, Value v)
                       => ConcurrentMeta k v
                       -> Page 'TypeConcurrentMeta
    OverflowPage :: (Value v)
                 => v
                 -> Page 'TypeOverflow
    LeafNodePage  :: (Key k, Value v)
                  => Height 'Z
                  -> Node 'Z k v
                  -> Page 'TypeLeafNode
    IndexNodePage  :: (Key k, Value v)
                   => Height ('S h)
                   -> Node ('S h) k v
                   -> Page 'TypeIndexNode

-- | A decoder with its type.
data SGet t = SGet (SPageType t) (Get (Page t))

-- | Get the type of a 'Page'.
pageType :: SPageType t -> PageType
pageType STypeEmpty          = TypeEmpty
pageType STypeConcurrentMeta = TypeConcurrentMeta
pageType STypeOverflow       = TypeOverflow
pageType STypeLeafNode       = TypeLeafNode
pageType STypeIndexNode      = TypeIndexNode

-- | Encode a page to a lazy byte string, but with the checksum set to zero.
encodeZeroChecksum :: Page t -> BL.ByteString
encodeZeroChecksum p = "\NUL\NUL\NUL\NUL" `BL.append` encodeNoChecksum p

-- | Encode a page to a lazy byte string, and prepend the calculated checksum.
encode :: Page t -> BL.ByteString
encode = prependChecksum . encodeNoChecksum

-- | Prepend the adler32 checksum of a bytestring to itself.
prependChecksum :: BL.ByteString -> BL.ByteString
prependChecksum bs = B.encode (adler32 bs :: Word32) `BL.append` bs

-- | Encode a page to a lazy byte string, without prepending the checksum.
encodeNoChecksum :: Page t -> BL.ByteString
encodeNoChecksum = runPut . putPage
  -- let bs = runPut (putPage p) in fromMaybe bs (tryCompress bs)
  where
    _tryCompress bs = do
        (t, body) <- BL.uncons bs
        c         <- compress (toStrict body)
        if BS.length c < fromIntegral (BL.length bs)
            then Just $ maskCompressed t `BL.cons` fromStrict c
            else Nothing

    maskCompressed t = t .|. 0x01

-- | Size of a node, if it were to be encoded.
encodedPageSize :: (Key k, Value v) => Height h -> Node h k v -> PageSize
encodedPageSize h = case viewHeight h of
        UZero -> fromIntegral . BL.length . encodeZeroChecksum . LeafNodePage h
        USucc _ -> fromIntegral . BL.length . encodeZeroChecksum . IndexNodePage h

-- | Decode a page, and verify the checksum.
decode :: SGet t -> ByteString -> Either String (Page t)
decode g@(SGet t _) bs = do
    let (cksumBs, body) = BS.splitAt 4 bs
    cksum <- if BS.length cksumBs < 4
                then Left $ "could not decode " ++ show (pageType t) ++ ": "
                        ++ "not enough checksum bytes"
                else Right $ B.decode (fromStrict cksumBs)
    let cksum' = adler32 body
    if cksum' /= cksum
        then Left $ "could not decode " ++ show (pageType t) ++ ": "
                 ++ "expected checksum " ++ show cksum' ++ " but checksum "
                 ++ "field contains " ++ show cksum
        else decodeNoChecksum g body


-- | Decode a page with a specific decoder, or return the error.
decodeNoChecksum :: SGet t -> ByteString -> Either String (Page t)
decodeNoChecksum (SGet t g) bs = case runGetOrFail g (fromStrict bs) of
    Left  err       -> Left $ err' err
    Right (_, _, v) -> Right v
  where
    err' (bs', offset, err) =
        "could not decode " ++ show (pageType t) ++ ": " ++ err ++
        "at pos " ++ show offset ++ ", remaining bytes: " ++ show bs' ++
        ", full body: " ++ show bs

    _decompressed = fromMaybe (fromStrict bs) $ do
        (tb, body) <- BS.uncons bs
        if isCompressed tb
            then do
                c <- decompress body
                Just $ unmaskCompressed tb `BL.cons` fromStrict c
            else Nothing

    isCompressed b = b .&. 0x01 == 0x01
    unmaskCompressed b = b .&. 0xFE


-- | Monadic wrapper around 'decode'
decodeM :: MonadThrow m => SGet t -> ByteString -> m (Page t)
decodeM g bs = case decode g bs of
    Left err -> throwM $ DecodeError err
    Right v -> return v

-- | The encoder of a 'Page'.
putPage :: Page t -> Put
putPage EmptyPage              = put TypeEmpty
putPage (ConcurrentMetaPage m) = put TypeConcurrentMeta >> put m
putPage (OverflowPage v)       = put TypeOverflow >> put v
putPage (LeafNodePage _ n)     = put TypeLeafNode >> putLeafNode n
putPage (IndexNodePage h n)    = put TypeIndexNode >> put h >> putIndexNode n

-- | Decoder for an empty page.
emptyPage :: SGet 'TypeEmpty
emptyPage = SGet STypeEmpty $ get >>= \case
    TypeEmpty -> return EmptyPage
    x -> fail $ "unexpected " ++ show x ++ " while decoding TypeEmpty"

-- | Decoder for a leaf node page.
leafNodePage :: (Key k, Value v)
             => Height 'Z
             -> Proxy k
             -> Proxy v
             -> SGet 'TypeLeafNode
leafNodePage h k v = SGet STypeLeafNode $ get >>= \case
    TypeLeafNode -> LeafNodePage h <$> get' h k v
    x -> fail $ "unexpected " ++ show x ++ " while decoding TypeLeafNode"
  where
    get' :: (Key k, Value v)
         => Height 'Z -> Proxy k -> Proxy v -> Get (Node 'Z k v)
    get' h' _ _ = getLeafNode h'

-- | Decoder for a leaf node page.
indexNodePage :: (Key k, Value v)
              => Height ('S n)
              -> Proxy k
              -> Proxy v
              -> SGet 'TypeIndexNode
indexNodePage h k v = SGet STypeIndexNode $ get >>= \case
    TypeIndexNode -> do
        h' <- get
        if fromHeight h == fromHeight h'
            then IndexNodePage h <$> get' h k v
            else fail $ "expected height " ++ show h ++ " but got "
                     ++ show h' ++ " while decoding TypeNode"
    x -> fail $ "unexpected " ++ show x ++ " while decoding TypeIndexNode"
  where
    get' :: (Key k, Value v)
         => Height ('S n) -> Proxy k -> Proxy v -> Get (Node ('S n) k v)
    get' h' _ _ = getIndexNode h'

-- | Decoder for an overflow page.
overflowPage :: (Value v) => Proxy v -> SGet 'TypeOverflow
overflowPage v = SGet STypeOverflow $ get >>= \case
    TypeOverflow -> OverflowPage <$> get' v
    x -> fail $ "unexpected " ++ show x ++ " while decoding TypeOverflow"
  where
    get' :: (Value v) => Proxy v -> Get v
    get' _ = get

concurrentMetaPage :: (Key k, Value v)
                   => Proxy k
                   -> Proxy v
                   -> SGet 'TypeConcurrentMeta
concurrentMetaPage k v = SGet STypeConcurrentMeta $ get >>= \ case
    TypeConcurrentMeta -> ConcurrentMetaPage <$> get' k v
    x -> fail $ "unexpected " ++ show x ++ " while decoding TypeConcurrentMeta"
  where
    get' :: (Key k, Value v) => Proxy k -> Proxy v -> Get (ConcurrentMeta k v)
    get' _ _ = get

-- | Exception thrown when decoding of a page fails.
newtype DecodeError = DecodeError String deriving (Show, Typeable)

instance Exception DecodeError where
