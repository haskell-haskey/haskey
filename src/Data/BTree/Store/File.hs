{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}
module Data.BTree.Store.File where

import Control.Monad.IO.Class
import Control.Monad.State.Class
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.State.Strict ( StateT, evalStateT, execStateT
                                        , runStateT
                                        )

import Data.Binary (Binary(..), Put, Get)
import Data.ByteString (ByteString, hGet, hPut)
import Data.ByteString.Lazy (fromStrict, toStrict)
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
 - If the page is larger than the maximum page size, the encoeded byte string
 - will be returned using 'Left'.
 -}
encode :: PageSize -> Page -> Either ByteString ByteString
encode size page
    | Just n <- padding = Right . toStrict $
        B.runPut (B.put enc) <> BL.replicate n 0
    | otherwise = Left . toStrict $
        B.runPut (B.put enc)
  where
    enc = B.runPut (putPage page)
    padding | n <- fromIntegral size - BL.length enc, n >= 0 = Just n
            | otherwise = Nothing

decode :: Get a -> ByteString -> a
decode g = B.runGet g . B.runGet B.get . fromStrict


deriving instance Show Page

data BPage = BPageMeta | BPageEmpty | BPageNode | BPageAppendMeta
           deriving (Eq, Generic)
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

runStore :: StoreT FilePath IO a -> (Maybe a, Files FilePath)
runStore = undefined

nodeIdToPageId :: NodeId height key val -> PageId
nodeIdToPageId (NodeId n) = PageId n

pageIdToNodeId :: PageId -> NodeId height key val
pageIdToNodeId (PageId n) = NodeId n

--------------------------------------------------------------------------------

instance (Ord fp, Applicative m, Monad m, MonadIO m) =>
    StoreM fp (StoreT fp m)
  where
    nodePageSize = do
        pageSize <- maxPageSize
        return $ \h ->
            fromIntegral . BS.length . either id id . encode pageSize . PageNode h

    maxPageSize = return 128

    getSize fp = do
        handle     <- StoreT . MaybeT $ gets (M.lookup fp)
        PageMeta c <- StoreT $ readPageMeta handle
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

readPageMeta :: MonadIO m => Handle -> m Page
readPageMeta = undefined

writePage :: MonadIO m => Handle -> PageId -> Page -> PageSize -> m ()
writePage h (PageId pid) page size = liftIO $ do
    hSeek h AbsoluteSeek (fromIntegral $ pid * fromIntegral size)
    hPut h $ either (fail "writePage: page too large") id (encode size page)

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
