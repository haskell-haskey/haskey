{-# LANGUAGE DataKinds #-}
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
import Control.Monad.Except (MonadError, throwError)

import Data.Binary (Binary(..), Put, Get)
import Data.Binary.Get (runGetOrFail)
import Data.Binary.Put (runPut)
import Data.ByteString.Lazy (ByteString)
import Data.Proxy

import GHC.Generics (Generic)

import Data.BTree.Alloc.Concurrent
import Data.BTree.Impure
import Data.BTree.Impure.Structures (getNode, putNode)
import Data.BTree.Primitives

-- | The type of a page.
data PageType = TypeEmpty
              | TypeNode
              | TypeConcurrentMeta
              deriving (Eq, Generic, Show)

instance Binary PageType where

-- | A decoded page, of a certain type @t@ of kind 'PageType'.
data Page (t :: PageType) where
    EmptyPage :: Page 'TypeEmpty
    NodePage  :: (Key k, Value v)
              => Height h
              -> Node h k v
              -> Page 'TypeNode
    ConcurrentMetaPage :: (Key k, Value v)
                       => ConcurrentMeta k v
                       -> Page 'TypeConcurrentMeta

-- | Encode a page to a lazy byte string.
encode :: Page t -> ByteString
encode = runPut . putPage

-- | Decode a page with a specific decoder, or return the error.
decode :: Get a -> ByteString -> Either String a
decode g bs = case runGetOrFail g bs of
    Left  (_, _, err) -> Left err
    Right (_, _, v)   -> Right v

-- | Monadic wrapper around 'decode'
decodeM :: MonadError String m => Get a -> ByteString -> m a
decodeM g bs = case decode g bs of
    Left err -> throwError err
    Right v -> return v

-- | The encoder of a 'Page'.
putPage :: Page t -> Put
putPage EmptyPage              = put TypeEmpty
putPage (NodePage h n)         = put TypeNode >> put h >> putNode n
putPage (ConcurrentMetaPage m) = put TypeConcurrentMeta >> put m

-- | Decoder for an empty page.
emptyPage :: Get (Page 'TypeEmpty)
emptyPage = get >>= \case
    TypeEmpty -> return EmptyPage
    x -> fail $ "unexpected " ++ show x ++ " while decoding TypeEmpty"

-- | Decoder for a node page.
nodePage :: (Key k, Value v)
         => Height h
         -> Proxy k
         -> Proxy v
         -> Get (Page 'TypeNode)
nodePage h k v = get >>= \case
    TypeNode -> do
        h' <- get
        if fromHeight h == fromHeight h'
            then NodePage h <$> get' h k v
            else fail $ "expected height " ++ show h ++ " but got "
                     ++ show h ++ " while decoding TypeNode"
    x -> fail $ "unexpected " ++ show x ++ " while decoding TypeNode"
  where
    get' :: (Key k, Value v)
         => Height h -> Proxy k -> Proxy v -> Get (Node h k v)
    get' h' _ _ = getNode h'

concurrentMetaPage :: (Key k, Value v)
                   => Proxy k
                   -> Proxy v
                   -> Get (Page 'TypeConcurrentMeta)
concurrentMetaPage k v = get >>= \ case
    TypeConcurrentMeta -> ConcurrentMetaPage <$> get' k v
    x -> fail $ "unexpected " ++ show x ++ " while decoding TypeConcurrentMeta"
  where
    get' :: (Key k, Value v) => Proxy k -> Proxy v -> Get (ConcurrentMeta k v)
    get' _ _ = get
