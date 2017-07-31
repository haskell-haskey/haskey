{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-| On-disk storage back-end. Can be used as a storage back-end for the
   append-only page allocator (see "Data.BTree.Alloc").
 -}
module Data.BTree.Store.File (
  -- * Storage
  Page(..)
, Files
, StoreT
, runStoreT
, evalStoreT
, execStoreT
, emptyStore

  -- * Binary encoding
, encodeAndPad
, encode
, decodeMaybe
, decode
, getEmptyPage
, getPageNode
) where
import Control.Applicative (Applicative, (<$>), (<*>), pure)
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.State.Class
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.State.Strict ( StateT, evalStateT, execStateT , runStateT)

import Data.Binary (Binary, Put, Get)
import Data.ByteString (ByteString, hGet, hPut)
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Coerce (coerce)
import Data.Map (Map)
import Data.Maybe (fromMaybe)
import Data.Monoid ((<>))
import Data.Typeable
import qualified Data.Binary as B
import qualified Data.Binary.Get as B
import qualified Data.Binary.Put as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map as M

import GHC.Generics (Generic)

import System.IO

import Data.BTree.Alloc.Concurrent
import Data.BTree.Impure.Structures
import Data.BTree.Primitives
import Data.BTree.Store.Class

--------------------------------------------------------------------------------

{-| A decoded on-disk page. -}
data Page =
      PageEmpty
    | forall height key val. (Key key, Value val) =>
      PageNode (Height height) (Node height key val)
    | forall key val.        (Key key, Value val) =>
      PageConcurrentMeta (ConcurrentMeta key val)
    deriving (Typeable)

{-| Encode a page padding it to the maxim page size.

   Return 'Nothing' of the page is too large to fit into one page size.
 -}
encodeAndPad :: PageSize -> Page -> Maybe ByteString
encodeAndPad size page
    | Just n <- padding = Just . toStrict $
        enc <> BL.replicate n 0
    | otherwise = Nothing
  where
    enc = encode page
    padding | n <- fromIntegral size - BL.length enc, n >= 0 = Just n
            | otherwise = Nothing

{-| Encode a page, including the length of the encoded byte string -}
encode :: Page -> BL.ByteString
encode = B.runPut . putPage

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
decode g = fromMaybe (error "decoding unexpectedly failed") . decodeMaybe g


deriving instance Show Page

data BPage = BPageEmpty | BPageNode | BPageConcurrentMeta
           deriving (Eq, Generic, Show)
instance Binary BPage where

{-| The encoder of a page. -}
putPage :: Page -> Put
putPage PageEmpty = B.put BPageEmpty
putPage (PageNode h n) = B.put BPageNode >> B.put h >> putNode n
putPage (PageConcurrentMeta m) = B.put BPageConcurrentMeta >> B.put m

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

{-| Decoder for a page containg 'ConcurrentMeta'. Will return a 'PageConcurrentMeta'. -}
getPageConcurrentMeta :: (Key key, Value val)
                  => Proxy key
                  -> Proxy val
                  -> Get Page
getPageConcurrentMeta k v = B.get >>= \case
    BPageConcurrentMeta -> PageConcurrentMeta <$> getConcurrentMeta' k v
    x                   -> fail $ "unexpected " ++ show x
  where
    getConcurrentMeta' :: (Key key, Value val)
                   => Proxy key
                   -> Proxy val
                   -> Get (ConcurrentMeta key val)
    getConcurrentMeta' _ _ = B.get

--------------------------------------------------------------------------------

{-| A collection of files, each associated with a certain @fp@ handle.

   Each file is a 'Handle' opened in 'System.IO.ReadWriteMode' and contains a
   collection of physical pages.
 -}
type Files fp = Map fp (Handle, PageCount)

getFileHandle :: (Handle, PageCount) -> Handle
getFileHandle = fst

getFilePageCount :: (Handle, PageCount) -> PageCount
getFilePageCount = snd

lookupHandle :: Ord fp => fp -> Files fp -> Maybe Handle
lookupHandle fp = (getFileHandle <$>) . M.lookup fp

{-| Monad in which on-disk storage operations can take place.

  Two important instances are 'StoreM' making it a storage back-end, and
  'ConcurrentMetaStoreM' making it a storage back-end compatible with the
  concurrent page allocator.
 -}
newtype StoreT fp m a = StoreT
    { fromStoreT :: MaybeT (StateT (Files fp) m) a
    } deriving (Applicative, Functor, Monad, MonadIO)

{-| Run the storage operations in the 'StoreT' monad, given a collection of
   open files.
 -}
runStoreT :: StoreT fp m a -> Files fp -> m (Maybe a, Files fp)
runStoreT = runStateT . runMaybeT . fromStoreT

{-| Evaluate the storage operations in the 'StoreT' monad, given a collection of
   open files.
 -}
evalStoreT :: Monad m => StoreT fp m a -> Files fp -> m (Maybe a)
evalStoreT = evalStateT . runMaybeT . fromStoreT

{-| Execute the storage operations in the 'StoreT' monad, given a collection of
   open files.
 -}
execStoreT :: Monad m => StoreT fp m a -> Files fp-> m (Files fp)
execStoreT = execStateT . runMaybeT . fromStoreT

{-| An empty file store, with no open files. -}
emptyStore :: Files fp
emptyStore = M.empty

--------------------------------------------------------------------------------

instance (Applicative m, Monad m, MonadIO m) =>
    StoreM FilePath (StoreT FilePath m)
  where
    openHandle fp = do
        alreadyOpen <- StoreT $ M.member fp <$> get
        pageSize    <- maxPageSize
        unless alreadyOpen $ StoreT $ do
            -- Open the file in rw mode
            fh <- liftIO $ openFile fp ReadWriteMode

            -- Calculate the number of pages
            liftIO $ hSeek fh SeekFromEnd 0
            pc <- liftIO $ quot <$> hTell fh <*> pure (fromIntegral pageSize)
            modify (M.insert fp (fh, fromIntegral pc))

    closeHandle fp = StoreT $ (lookupHandle fp <$> get) >>= \case
        Nothing -> return ()
        Just fh -> do
            liftIO $ hClose fh
            modify (M.delete fp)

    nodePageSize =
        return $ \h ->
            fromIntegral . BL.length . encode . PageNode h

    maxPageSize = return 4096

    getSize fp = do
        f <- StoreT . MaybeT $ gets (M.lookup fp)
        return (getFilePageCount f)

    setSize fp pc = do
        h <- StoreT . MaybeT $ gets (lookupHandle fp)
        StoreT $ modify (M.insert fp (h, pc))

    getNodePage fp height key val nid = do
        handle   <- StoreT . MaybeT $ gets (lookupHandle fp)
        pageSize <- maxPageSize
        PageNode hgtSrc tree <- StoreT $ readPageNode handle
                                                      height key val
                                                      (nodeIdToPageId nid)
                                                      pageSize
        StoreT . MaybeT $ return (castNode hgtSrc height tree)

    putNodePage fp hgt nid node = do
        handle   <- StoreT . MaybeT $ gets (lookupHandle fp)
        pageSize <- maxPageSize
        StoreT $ writePage handle
                           (nodeIdToPageId nid)
                           (PageNode hgt node)
                           pageSize


--------------------------------------------------------------------------------

writePage :: MonadIO m => Handle -> PageId -> Page -> PageSize -> m ()
writePage h (PageId pid) page size = liftIO $ do
    hSeek h AbsoluteSeek (fromIntegral $ pid * fromIntegral size)
    hPut h =<< maybe (fail "writePage: page too large") return (encodeAndPad size page)

readPageNode :: (MonadIO m, Key key, Value val)
             => Handle
             -> Height height
             -> Proxy key
             -> Proxy val
             -> PageId
             -> PageSize
             -> m Page
readPageNode h hgt k v (PageId pid) size = liftIO $ do
    hSeek h AbsoluteSeek (fromIntegral $ pid * fromIntegral size)
    bs <- hGet h (fromIntegral size)
    return $ decode (getPageNode hgt k v) bs

--------------------------------------------------------------------------------

instance (Applicative m, Monad m, MonadIO m) =>
    ConcurrentMetaStoreM FilePath (StoreT FilePath m)
  where
    putConcurrentMeta fp i meta = do
        handle   <- StoreT . MaybeT $ gets (lookupHandle fp)
        pageSize <- maxPageSize
        StoreT $ writePage handle i
                           (PageConcurrentMeta meta)
                           pageSize

    openConcurrentMeta fps k v = do
        fh1 <- StoreT . MaybeT $ gets (lookupHandle $ concurrentHandlesMetadata1 fps)
        fh2 <- StoreT . MaybeT $ gets (lookupHandle $ concurrentHandlesMetadata2 fps)

        pageSize <- maxPageSize
        meta1 <- StoreT $ readConcurrentMetaNode fh1 k v 0 pageSize
        meta2 <- StoreT $ readConcurrentMetaNode fh2 k v 0 pageSize
        case (meta1, meta2) of
            (Nothing, Nothing) -> return Nothing
            (Just m , Nothing) -> do PageConcurrentMeta meta <- return m
                                     return . Just $! coerce meta
            (Nothing, Just m ) -> do PageConcurrentMeta meta <- return m
                                     return . Just $! coerce meta
            (Just m , Just n ) -> do
                PageConcurrentMeta x <- return m
                PageConcurrentMeta y <- return n
                if concurrentMetaRevision x > concurrentMetaRevision y
                    then return . Just $! coerce x
                    else return . Just $! coerce y


--------------------------------------------------------------------------------

readConcurrentMetaNode :: (MonadIO m, Key key, Value val)
                       => Handle
                       -> Proxy key
                       -> Proxy val
                       -> PageId
                       -> PageSize
                       -> m (Maybe Page)
readConcurrentMetaNode h k v (PageId pid) size = liftIO $ do
    hSeek h AbsoluteSeek (fromIntegral $ pid * fromIntegral size)
    bs <- hGet h (fromIntegral size)
    return $ decodeMaybe (getPageConcurrentMeta k v) bs

--------------------------------------------------------------------------------
