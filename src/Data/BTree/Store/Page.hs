{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
-- | This module contains structures and functions to decode and encode
-- pages.
module Data.BTree.Store.Page where

import Control.Applicative ((<$>))
import Control.Monad.Catch

import Data.Binary (Binary(..), Put, Get)
import Data.Binary.Get (runGetOrFail)
import Data.Binary.Put (runPut)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Proxy
import Data.Typeable (Typeable)

import GHC.Generics (Generic)

import Data.BTree.Alloc.Concurrent
import Data.BTree.Impure
import Data.BTree.Impure.Structures (getNode, putNode)
import Data.BTree.Primitives

-- | The type of a page.
data PageType = TypeEmpty
              | TypeNode
              | TypeOverflow
              | TypeConcurrentMeta
              deriving (Eq, Generic, Show)

data SPageType t where
    STypeEmpty           :: SPageType 'TypeEmpty
    STypeNode            :: SPageType 'TypeNode
    STypeOverflow        :: SPageType 'TypeOverflow
    STypeConcurrentMeta  :: SPageType 'TypeConcurrentMeta

instance Binary PageType where

-- | A decoded page, of a certain type @t@ of kind 'PageType'.
data Page (t :: PageType) where
    EmptyPage :: Page 'TypeEmpty
    NodePage  :: (Key k, Value v)
              => Height h
              -> Node h k v
              -> Page 'TypeNode
    OverflowPage :: (Value v)
                 => v
                 -> Page 'TypeOverflow
    ConcurrentMetaPage :: (Key k, Value v)
                       => ConcurrentMeta k v
                       -> Page 'TypeConcurrentMeta

-- | A decoder with its type.
data SGet t = SGet (SPageType t) (Get (Page t))

-- | Get the type of a 'Page'.
pageType :: SPageType t -> PageType
pageType STypeEmpty          = TypeEmpty
pageType STypeNode           = TypeNode
pageType STypeOverflow       = TypeOverflow
pageType STypeConcurrentMeta = TypeConcurrentMeta

-- | Encode a page to a lazy byte string.
encode :: Page t -> ByteString
encode = toStrict . runPut . putPage

-- | Decode a page with a specific decoder, or return the error.
decode :: SGet t -> ByteString -> Either String (Page t)
decode (SGet t g) bs = case runGetOrFail g (fromStrict bs) of
    Left  err       -> Left $ err' err
    Right (_, _, v) -> Right v
  where
    err' (bs', offset, err) =
        "could not decode " ++ show (pageType t) ++ ": " ++ err ++
        "at pos " ++ show offset ++ ", remaining bytes: " ++ show bs' ++
        ", full body: " ++ show bs


-- | Monadic wrapper around 'decode'
decodeM :: MonadThrow m => SGet t -> ByteString -> m (Page t)
decodeM g bs = case decode g bs of
    Left err -> throwM $ DecodeError err
    Right v -> return v

-- | The encoder of a 'Page'.
putPage :: Page t -> Put
putPage EmptyPage              = put TypeEmpty
putPage (NodePage h n)         = put TypeNode >> put h >> putNode n
putPage (OverflowPage v)       = put TypeOverflow >> put v
putPage (ConcurrentMetaPage m) = put TypeConcurrentMeta >> put m

-- | Decoder for an empty page.
emptyPage :: SGet 'TypeEmpty
emptyPage = SGet STypeEmpty $ get >>= \case
    TypeEmpty -> return EmptyPage
    x -> fail $ "unexpected " ++ show x ++ " while decoding TypeEmpty"

-- | Decoder for a node page.
nodePage :: (Key k, Value v)
         => Height h
         -> Proxy k
         -> Proxy v
         -> SGet 'TypeNode
nodePage h k v = SGet STypeNode $ get >>= \case
    TypeNode -> do
        h' <- get
        if fromHeight h == fromHeight h'
            then NodePage h <$> get' h k v
            else fail $ "expected height " ++ show h ++ " but got "
                     ++ show h' ++ " while decoding TypeNode"
    x -> fail $ "unexpected " ++ show x ++ " while decoding TypeNode"
  where
    get' :: (Key k, Value v)
         => Height h -> Proxy k -> Proxy v -> Get (Node h k v)
    get' h' _ _ = getNode h'

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
