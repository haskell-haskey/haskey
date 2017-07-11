{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
module Data.BTree.Store.File where

import Control.Applicative ((<$>))
import Control.Monad.IO.Class
import Control.Monad.State.Class
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.State.Strict ( StateT, evalStateT, execStateT
                                        , runStateT
                                        )

import Data.Binary (Binary(..), Put, Get)
import Data.ByteString (ByteString, hGet, hPut)
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Coerce (coerce)
import Data.Map (Map)
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
import Data.BTree.Primitives
import Data.BTree.Store.Class

--------------------------------------------------------------------------------

data Page =
      PageMeta PageCount
    | PageEmpty
    | forall height key val. (Key key, Value val) =>
      PageNode (Height height) (Node height key val)
    | forall key val.        (Key key, Value val) =>
      PageAppendMeta (AppendMeta key val)
    deriving (Typeable)

{-| Encode a page padding it to the maxim page size.
 -
 - Return 'Nothing' of the page is too large to fit into one page size.
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
encode = B.runPut . B.put . B.runPut . putPage

decode :: Get a -> ByteString -> a
decode g = B.runGet g . B.runGet B.get . fromStrict


deriving instance Show Page

data BPage = BPageMeta | BPageEmpty | BPageNode | BPageAppendMeta
           deriving (Eq, Generic, Show)
instance Binary BPage where

putPage :: Page -> Put
putPage (PageMeta c) = B.put BPageMeta >> B.put c
putPage PageEmpty = B.put BPageEmpty
putPage (PageNode h n) = B.put BPageNode >> B.put h >> putNode n
putPage (PageAppendMeta m) = B.put BPageAppendMeta >> B.put m

getMetaPage :: Get Page
getMetaPage = B.get >>= \BPageMeta -> PageMeta <$> B.get

getEmptyPage :: Get Page
getEmptyPage = B.get >>= \BPageEmpty -> return PageEmpty

getPageNode :: (Key key, Value val)
            => Height height
            -> Proxy key
            -> Proxy val
            -> Get Page
getPageNode h key val = B.get >>= \BPageNode -> do
    h' <- B.get
    if fromHeight h == fromHeight h'
        then PageNode h <$> getNode' h' key val
        else fail $ "expected height " ++ show h ++ " but got " ++ show h'
  where
    getNode' :: (Key key, Value val)
             => Height h
             -> Proxy key
             -> Proxy val
             -> Get (Node h key val)
    getNode' h' _ _ = getNode h'

getPageAppendMeta :: (Key key, Value val)
                  => Proxy key
                  -> Proxy val
                  -> Get Page
getPageAppendMeta k v = B.get >>= \BPageAppendMeta ->
    PageAppendMeta <$> getAppendMeta' k v
  where
    getAppendMeta' :: (Key key, Value val)
                   => Proxy key
                   -> Proxy val
                   -> Get (AppendMeta key val)
    getAppendMeta' _ _ = B.get

--------------------------------------------------------------------------------

type Files fp = Map fp Handle

newtype StoreT fp m a = StoreT
    { fromStoreT :: MaybeT (StateT (Files fp) m) a
    } deriving (Applicative, Functor, Monad)

runStoreT :: StoreT fp m a -> Files fp -> m (Maybe a, Files fp)
runStoreT = runStateT . runMaybeT . fromStoreT

evalStoreT :: Monad m => StoreT fp m a -> Files fp -> m (Maybe a)
evalStoreT = evalStateT . runMaybeT . fromStoreT

execStoreT :: Monad m => StoreT fp m a -> Files fp-> m (Files fp)
execStoreT = execStateT . runMaybeT . fromStoreT

runStore :: FilePath -> StoreT FilePath IO a -> IO (Maybe a, Files FilePath)
runStore fp action = withFile fp ReadWriteMode $ \handle -> do
    let files = M.fromList [(fp, handle)]
    runStateT (runMaybeT (fromStoreT action)) files

nodeIdToPageId :: NodeId height key val -> PageId
nodeIdToPageId (NodeId n) = PageId n

pageIdToNodeId :: PageId -> NodeId height key val
pageIdToNodeId (PageId n) = NodeId n

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
        PageAppendMeta meta <-
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

--------------------------------------------------------------------------------

readAppendMetaNode :: (MonadIO m, Key key, Value val)
                   => Handle
                   -> Proxy key
                   -> Proxy val
                   -> PageId
                   -> PageSize
                   -> m Page
readAppendMetaNode h k v (PageId pid) size = liftIO $ do
    hSeek h AbsoluteSeek (fromIntegral $ pid * fromIntegral size)
    bs <- hGet h (fromIntegral size)
    return $ decode (getPageAppendMeta k v) bs

--------------------------------------------------------------------------------
