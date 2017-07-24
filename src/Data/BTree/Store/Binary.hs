{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-| Binary in-memory storage back-end. Can be used as a storage back-end for
   the append-only page allocator (see "Data.BTree.Alloc").
 -}
module Data.BTree.Store.Binary (
  -- * Storage
  Page(..)
, File
, Files
, StoreT
, runStoreT
, evalStoreT
, execStoreT
, runStore
, evalStore
, emptyStore

  -- * Binary encoding
, encode
, decodeMaybe
, decode
, getEmptyPage
, getPageNode
, getPageAppendMeta
) where

import Control.Applicative (Applicative(..), (<$>))
import Control.Monad
import Control.Monad.Identity
import Control.Monad.State.Class
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.State.Strict ( StateT, evalStateT, execStateT , runStateT)

import Data.Binary (Binary(..), Put, Get)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Coerce
import Data.Map (Map)
import Data.Proxy
import Data.Typeable
import qualified Data.Binary as B
import qualified Data.Binary.Get as B
import qualified Data.Binary.Put as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map as M

import GHC.Generics (Generic)

import Data.BTree.Alloc.Append
import Data.BTree.Alloc.PageReuse
import Data.BTree.Impure.Structures
import Data.BTree.Primitives
import Data.BTree.Store.Class

--------------------------------------------------------------------------------

{-| A decoded binary page. -}
data Page = PageEmpty
    | forall height key val. (Key key, Value val) =>
      PageNode (Height height) (Node height key val)
    | forall key val.        (Key key, Value val) =>
      PageAppendMeta (AppendMeta key val)
    | forall key val.        (Key key, Value val) =>
      PagePageReuseMeta (PageReuseMeta key val)
    deriving (Typeable)

{-| Encode a page. -}
encode :: Page -> ByteString
encode = toStrict . B.runPut . putPage

{-| Decode a page with the specified decoder, or return 'Nothing'. -}
decodeMaybe :: Get a -> ByteString -> Maybe a
decodeMaybe g bs =
    case B.runGetOrFail g . fromStrict $ bs of
        Left _ -> Nothing
        Right (_, _, a) -> Just a

{-| Decode a page with the specified decoder.

   This function is partial, and will fail when the data cannot be decoded.
 -}
decode :: Get a -> ByteString -> a
decode g = B.runGet g . fromStrict

deriving instance Show Page

data BPage = BPageEmpty | BPageNode | BPageAppendMeta | BPagePageReuseMeta
           deriving (Eq, Generic, Show)
instance Binary BPage where

{-| The encoder of a page. -}
putPage :: Page -> Put
putPage PageEmpty = B.put BPageEmpty
putPage (PageNode h n) = B.put BPageNode >> B.put h >> putNode n
putPage (PageAppendMeta m) = B.put BPageAppendMeta >> B.put m
putPage (PagePageReuseMeta m) = B.put BPagePageReuseMeta >> B.put m

{-| Decoder for empty pages. Will return a 'PageEmpty'. -}
getEmptyPage :: Get Page
getEmptyPage = B.get >>= \case
    BPageEmpty -> return PageEmpty
    x          -> fail $ "unexpected " ++ show x

{-| Decoder for a page containing a 'Node'. Will return a 'PageNode'. -}
getPageNode :: (Key key, Value val)
            => Height height
            -> Proxy key
            -> Proxy val
            -> Get Page
getPageNode h key val = B.get >>= \case
    BPageNode -> do
        h' <- B.get
        if fromHeight h == fromHeight h'
            then PageNode h <$> getNode' h' key val
            else fail $ "expected height " ++ show h ++ " but got " ++ show h'
    x -> fail $ "unexpected " ++ show x
  where
    getNode' :: (Key key, Value val)
             => Height h
             -> Proxy key
             -> Proxy val
             -> Get (Node h key val)
    getNode' h' _ _ = getNode h'

{-| Decoder for a page containg 'AppendMeta'. Will return a 'PageAppendMeta'. -}
getPageAppendMeta :: (Key key, Value val)
                  => Proxy key
                  -> Proxy val
                  -> Get Page
getPageAppendMeta k v = B.get >>= \case
    BPageAppendMeta -> PageAppendMeta <$> getAppendMeta' k v
    x               -> fail $ "unexpected " ++ show x
  where
    getAppendMeta' :: (Key key, Value val)
                   => Proxy key
                   -> Proxy val
                   -> Get (AppendMeta key val)
    getAppendMeta' _ _ = B.get

{-| Decoder for a page containg 'PageReuseMeta'. Will return a 'PagePageReuseMeta'. -}
getPagePageReuseMeta :: (Key key, Value val)
                  => Proxy key
                  -> Proxy val
                  -> Get Page
getPagePageReuseMeta k v = B.get >>= \case
    BPagePageReuseMeta -> PagePageReuseMeta <$> getPageReuseMeta' k v
    x               -> fail $ "unexpected " ++ show x
  where
    getPageReuseMeta' :: (Key key, Value val)
                   => Proxy key
                   -> Proxy val
                   -> Get (PageReuseMeta key val)
    getPageReuseMeta' _ _ = B.get

--------------------------------------------------------------------------------

{-| A file containing a collection of pages. -}
type File     = Map PageId ByteString

{-| A collection of 'File's, each associated with a certain @fp@ handle. -}
type Files fp = Map fp File

{-| Monad in which binary storage operations can take place.

   Two important instances are 'StoreM' making it a storage back-end, and
   'AppendMetaStoreM' making it a storage back-end compatible with the
   append-only page allocator.
 -}
newtype StoreT fp m a = StoreT
    { fromStoreT :: MaybeT (StateT (Files fp) m) a
    } deriving (Applicative, Functor, Monad)

{-| Run the storage operations in the 'StoreT' monad, given a collection of
   'File's.
 -}
runStoreT :: StoreT fp m a -> Files fp -> m (Maybe a, Files fp)
runStoreT = runStateT . runMaybeT . fromStoreT

{-| Evaluate the storage operations in the 'StoreT' monad, given a colletion of
   'File's.
 -}
evalStoreT :: Monad m => StoreT fp m a -> Files fp -> m (Maybe a)
evalStoreT = evalStateT . runMaybeT . fromStoreT

{-| Execute the storage operations in the 'StoreT' monad, given a colletion of
   'File's.
 -}
execStoreT :: Monad m => StoreT fp m a -> Files fp-> m (Files fp)
execStoreT = execStateT . runMaybeT . fromStoreT

{-| Run the storage operations in the 'StoreT' monad, without any other
   underlying monad, and with the empty store as the initial store. -}
runStore :: StoreT String Identity a -> (Maybe a, Files String)
runStore = runIdentity . flip runStoreT initialStore
  where initialStore = M.fromList [("Main", M.empty)]

{-| Evaluate the storage operations in the 'StoreT' monad, without any other
   underlying monad, and with the empty store as the initial store. -}
evalStore :: StoreT String Identity a -> Maybe a
evalStore = fst . runStore

{-| Construct a store with an empty database with name of type @hnd@. -}
emptyStore :: Ord hnd => hnd -> Files hnd
emptyStore hnd = M.fromList [(hnd, M.empty)]

--------------------------------------------------------------------------------

instance (Ord fp, Applicative m, Monad m) =>
    StoreM fp (StoreT fp m)
  where
    openHandle fp = StoreT $
        modify (M.insertWith (flip const) fp M.empty)

    closeHandle fp = StoreT $
        modify (M.delete fp)

    nodePageSize = return $ \h ->
        fromIntegral . BL.length . B.runPut . putPage . PageNode h

    maxPageSize = return 1024

    setSize fp (PageCount n) = StoreT $ do
        let emptyFile = M.fromList
                        [ (PageId i, encode PageEmpty)
                        | i <- [0..n-1]
                        ]
            res file  = M.intersection (M.union file emptyFile) emptyFile
        modify (M.update (Just . res) fp)

    getNodePage hnd height key val nid = StoreT $ do
        Just bs <-
            gets (M.lookup hnd >=> M.lookup (nodeIdToPageId nid))
        PageNode heightSrc tree <-
            return (decode (getPageNode height key val) bs)
        MaybeT $ return (castNode heightSrc height tree)

    putNodePage hnd height nid node = StoreT $
        modify (M.update (Just . M.insert (nodeIdToPageId nid) (encode
                    (PageNode height node))) hnd)

    getSize hnd = StoreT $
        fromIntegral . M.size <$> MaybeT (gets (M.lookup hnd))

--------------------------------------------------------------------------------

instance (Ord fp, Applicative m, Monad m) =>
    AppendMetaStoreM fp (StoreT fp m)
  where
    getAppendMeta h key val i = StoreT $ do
        Just bs <- gets (M.lookup h >=> M.lookup i)
        PageAppendMeta meta <- return (decode (getPageAppendMeta key val) bs)
        return $! coerce meta

    putAppendMeta h i meta = StoreT $
        modify (M.update (Just. M.insert i (encode (PageAppendMeta meta))) h)

    openAppendMeta hnd k v = do
        numPages <- getSize hnd
        page <- StoreT $ go $ PageId (fromPageCount (numPages - 1))
        case page of
            Nothing -> return Nothing
            Just x -> do
                (PageAppendMeta meta, pid) <- return x
                return $ Just (coerce meta, pid)
      where
        go pid = do
            Just bs <- gets (M.lookup hnd >=> M.lookup pid)
            case decodeMaybe (getPageAppendMeta k v) bs of
                Nothing -> if pid == 0 then return Nothing else go (pid - 1)
                Just x -> return $ Just (x, pid)

instance (Ord fp, Applicative m, Monad m) =>
    PageReuseMetaStoreM fp (StoreT fp m)
  where
    getPageReuseMeta h key val i = StoreT $ do
        Just bs <- gets (M.lookup h >=> M.lookup i)
        PagePageReuseMeta meta <- return (decode (getPagePageReuseMeta key val) bs)
        return $! coerce meta

    putPageReuseMeta h i meta = StoreT $
        modify (M.update (Just. M.insert i (encode (PagePageReuseMeta meta))) h)

    openPageReuseMeta hnd k v = do
        numPages <- getSize hnd
        page <- StoreT $ go $ PageId (fromPageCount (numPages - 1))
        case page of
            Nothing -> return Nothing
            Just x -> do
                (PagePageReuseMeta meta, pid) <- return x
                return $ Just (coerce meta, pid)
      where
        go pid = do
            Just bs <- gets (M.lookup hnd >=> M.lookup pid)
            case decodeMaybe (getPagePageReuseMeta k v) bs of
                Nothing -> if pid == 0 then return Nothing else go (pid - 1)
                Just x -> return $ Just (x, pid)

--------------------------------------------------------------------------------
