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
, runStore
, evalStore

  -- * Binary encoding
, encodeAndPad
, encode
, decodeMaybe
, decode
, getMetaPage
, getEmptyPage
, getPageNode
, getPageAppendMeta
) where
import Control.Applicative (Applicative, (<$>))
import Control.Monad.IO.Class
import Control.Monad.State.Class
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.State.Strict ( StateT, evalStateT, execStateT , runStateT)

import Data.Binary (Binary(..), Put, Get)
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
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map as M

import GHC.Generics (Generic)

import System.IO

import Data.BTree.Alloc.Append
import Data.BTree.Impure.Structures
import Data.BTree.Primitives
import Data.BTree.Store.Class

--------------------------------------------------------------------------------

{-| A decoded on-disk page. -}
data Page =
      PageMeta PageCount
    | PageEmpty
    | forall height key val. (Key key, Value val) =>
      PageNode (Height height) (Node height key val)
    | forall key val.        (Key key, Value val) =>
      PageAppendMeta (AppendMeta key val)
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

data BPage = BPageMeta | BPageEmpty | BPageNode | BPageAppendMeta
           deriving (Eq, Generic, Show)
instance Binary BPage where

{-| The encoder of a page. -}
putPage :: Page -> Put
putPage (PageMeta c) = B.put BPageMeta >> B.put c
putPage PageEmpty = B.put BPageEmpty
putPage (PageNode h n) = B.put BPageNode >> B.put h >> putNode n
putPage (PageAppendMeta m) = B.put BPageAppendMeta >> B.put m

{-| Decoder for meta pages. Will return a 'PageMeta'. -}
getMetaPage :: Get Page
getMetaPage = B.get >>= \case
    BPageMeta -> PageMeta <$> B.get
    x         -> fail $ "unexpected " ++ show x

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

--------------------------------------------------------------------------------

{-| A collection of files, each associated with a certain @fp@ handle.

   Each file is a 'Handle' opened in 'System.IO.ReadWriteMode' and contains a
   collection of physical pages.
 -}
type Files fp = Map fp Handle

{-| Monad in which on-disk storage operations can take place.

  Two important instances are 'StoreM' making it a storage back-end, and
  'AppendMetaStoreM' making it a storage back-end compatible with the
  append-only page allocator.
 -}
newtype StoreT fp m a = StoreT
    { fromStoreT :: MaybeT (StateT (Files fp) m) a
    } deriving (Applicative, Functor, Monad)

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

{-| Run the storage operations in the 'StoreT' monad, given a single open file
   that can be used as store the physical pages.

   The given 'Handle' should be opened in 'System.IO.ReadWriteMode'.
 -}
runStore :: FilePath -> Handle -> StoreT FilePath IO a -> IO (Maybe a, Files FilePath)
runStore fp handle action = do
    let files = M.fromList [(fp, handle)]
    runStateT (runMaybeT (fromStoreT action)) files

{-| Evaluate the storage operations in the 'StoreT' monad, given a single open
   file that can be used as store the physical pages.

   The given 'Handle' should be opened in 'System.IO.ReadWriteMode'.
 -}
evalStore :: FilePath -> Handle -> StoreT FilePath IO a -> IO (Maybe a)
evalStore fp handle action = fst <$> runStore fp handle action

--------------------------------------------------------------------------------

instance (Ord fp, Applicative m, Monad m, MonadIO m) =>
    StoreM fp (StoreT fp m)
  where
    nodePageSize =
        return $ \h ->
            fromIntegral . BL.length . encode . PageNode h

    maxPageSize = return 256

    getSize fp = do
        handle     <- StoreT . MaybeT $ gets (M.lookup fp)
        pageSize   <- maxPageSize
        PageMeta c <- StoreT $ readPageMeta handle pageSize
        return c

    setSize fp pc = do
        handle   <- StoreT . MaybeT $ gets (M.lookup fp)
        pageSize <- maxPageSize
        StoreT $ writePage handle
                           (PageId 0)
                           (PageMeta pc)
                           pageSize

    getNodePage fp height key val nid = do
        handle   <- StoreT . MaybeT $ gets (M.lookup fp)
        pageSize <- maxPageSize
        PageNode hgtSrc tree <- StoreT $ readPageNode handle
                                                      height key val
                                                      (nodeIdToPageId nid)
                                                      pageSize
        StoreT . MaybeT $ return (castNode hgtSrc height tree)

    putNodePage fp hgt nid node = do
        handle   <- StoreT . MaybeT $ gets (M.lookup fp)
        pageSize <- maxPageSize
        StoreT $ writePage handle
                           (nodeIdToPageId nid)
                           (PageNode hgt node)
                           pageSize


--------------------------------------------------------------------------------

readPageMeta :: MonadIO m => Handle -> PageSize -> m Page
readPageMeta h size = liftIO $ do
    hSeek h AbsoluteSeek 0
    bs <- hGet h (fromIntegral size)
    if BS.length bs < fromIntegral size
        then -- Write starting amount of pages:
             -- this is 1, as the first one is the meta page!
             writePage h 0 (PageMeta (PageCount 1)) size
             >> readPageMeta h size
        else return $ decode getMetaPage bs

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

instance (Ord fp, Applicative m, Monad m, MonadIO m) =>
    AppendMetaStoreM fp (StoreT fp m)
  where
    getAppendMeta fp key val i = do
        handle   <- StoreT . MaybeT $ gets (M.lookup fp)
        pageSize <- maxPageSize
        Just (PageAppendMeta meta) <-
            StoreT $ readAppendMetaNode handle
                                        key val
                                        i pageSize
        return $! coerce meta

    putAppendMeta fp i meta = do
        handle   <- StoreT . MaybeT $ gets (M.lookup fp)
        pageSize <- maxPageSize
        StoreT $ writePage handle i
                           (PageAppendMeta meta)
                           pageSize

    openAppendMeta fp k v = do
        handle   <- StoreT . MaybeT $ gets (M.lookup fp)
        numPages <- getSize fp
        pageSize <- maxPageSize
        page <- StoreT $ go handle pageSize $ PageId (fromPageCount (numPages - 1))
        case page of
            Nothing -> return Nothing
            Just x -> do
                (PageAppendMeta meta, pid) <- return x
                return $ Just (coerce meta, pid)
      where
        go _ _ 0 = return Nothing
        go h ps pid = readAppendMetaNode h k v pid ps >>= \case
            Just x -> return $ Just (x, pid)
            Nothing -> go h ps (pid - 1)

--------------------------------------------------------------------------------

readAppendMetaNode :: (MonadIO m, Key key, Value val)
                   => Handle
                   -> Proxy key
                   -> Proxy val
                   -> PageId
                   -> PageSize
                   -> m (Maybe Page)
readAppendMetaNode h k v (PageId pid) size = liftIO $ do
    hSeek h AbsoluteSeek (fromIntegral $ pid * fromIntegral size)
    bs <- hGet h (fromIntegral size)
    return $ decodeMaybe (getPageAppendMeta k v) bs

--------------------------------------------------------------------------------
