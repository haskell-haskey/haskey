{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | On-disk storage back-end. Can be used as a storage back-end for the
-- append-only page allocator (see "Data.BTree.Alloc").
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
) where
import Control.Applicative (Applicative, (<$>))
import Control.Monad
import Control.Monad.Except
import Control.Monad.State.Class
import Control.Monad.Trans.State.Strict ( StateT, evalStateT, execStateT , runStateT)

import Data.ByteString (ByteString)
import Data.Coerce (coerce)
import Data.Map (Map)
import Data.Monoid ((<>))
import qualified Data.ByteString as BS
import qualified Data.Map as M

import System.IO

import Data.BTree.Alloc.Concurrent
import Data.BTree.Impure.Structures
import Data.BTree.Primitives
import Data.BTree.Store.Class
import Data.BTree.Store.Page
import Data.BTree.Utils.Monad.Except (justErrM)

--------------------------------------------------------------------------------

-- | Encode a page padding it to the maxim page size.
--
-- Return 'Nothing' of the page is too large to fit into one page size.
encodeAndPad :: PageSize -> Page t -> Maybe ByteString
encodeAndPad size page
    | Just n <- padding = Just $
        enc <> BS.replicate n 0
    | otherwise = Nothing
  where
    enc = encode page
    padding | n <- fromIntegral size - BS.length enc, n >= 0 = Just n
            | otherwise = Nothing

--------------------------------------------------------------------------------

-- | A collection of files, each associated with a certain @fp@ handle.
--
-- Each file is a 'Handle' opened in 'System.IO.ReadWriteMode' and contains a
-- collection of physical pages.
type Files fp = Map fp Handle


lookupHandle :: (Ord fp, Show fp, Functor m, MonadError String m)
             => fp -> Files fp -> m Handle
lookupHandle fp m = justErrM ("no file for handle " ++ show fp) $ M.lookup fp m

-- | Monad in which on-disk storage operations can take place.
--
-- Two important instances are 'StoreM' making it a storage back-end, and
-- 'ConcurrentMetaStoreM' making it a storage back-end compatible with the
-- concurrent page allocator.
newtype StoreT fp m a = StoreT
    { fromStoreT :: ExceptT String (StateT (Files fp) m) a
    } deriving (Applicative, Functor, Monad, MonadIO, MonadState (Files fp), MonadError String)

-- | Run the storage operations in the 'StoreT' monad, given a collection of
-- open files.
runStoreT :: StoreT fp m a -> Files fp -> m (Either String a, Files fp)
runStoreT = runStateT . runExceptT . fromStoreT

-- | Evaluate the storage operations in the 'StoreT' monad, given a collection
-- of open files.
evalStoreT :: Monad m => StoreT fp m a -> Files fp -> m (Either String a)
evalStoreT = evalStateT . runExceptT . fromStoreT

-- | Execute the storage operations in the 'StoreT' monad, given a collection
-- of open files.
execStoreT :: Monad m => StoreT fp m a -> Files fp-> m (Files fp)
execStoreT = execStateT . runExceptT . fromStoreT

-- | An empty file store, with no open files.
emptyStore :: Files fp
emptyStore = M.empty

--------------------------------------------------------------------------------

instance (Applicative m, Monad m, MonadIO m) =>
    StoreM FilePath (StoreT FilePath m)
  where
    openHandle fp = do
        alreadyOpen <- M.member fp <$> get
        unless alreadyOpen $ do
            fh <- liftIO $ openFile fp ReadWriteMode
            modify $ M.insert fp fh

    closeHandle fp = do
        fh <- get >>= lookupHandle fp
        liftIO $ hClose fh
        modify (M.delete fp)

    nodePageSize =
        return $ \h ->
            fromIntegral . BS.length . encode . NodePage h

    maxPageSize = return 128

    newPageId fp = do
        fh <- get >>= lookupHandle fp
        fs <- liftIO $ hFileSize fh
        ps <- fromIntegral <$> maxPageSize

        let n = fs `div` ps
        case fs `rem` ps of
            0 -> return (fromIntegral n)
            _ -> return (fromIntegral n + 1)

    getNodePage fp height key val nid = do
        h    <- get >>= lookupHandle fp
        size <- maxPageSize

        let PageId pid = nodeIdToPageId nid
            offset     = fromIntegral $ pid * fromIntegral size

        liftIO $ hSeek h AbsoluteSeek offset
        bs <- liftIO $ BS.hGet h (fromIntegral size)
        n  <- decodeM (nodePage height key val) bs

        case n of
            NodePage hgtSrc tree ->
                justErrM "could not cast node page" $
                    castNode hgtSrc height tree

    putNodePage fp hgt nid node = do
        h    <- get >>= lookupHandle fp
        size <- maxPageSize

        let PageId pid = nodeIdToPageId nid
            offset     = fromIntegral $ pid * fromIntegral size
            page       = NodePage hgt node

        liftIO $ hSeek h AbsoluteSeek offset
        bs <- justErrM "putNodePage: page too large" $ encodeAndPad size page
        liftIO $ BS.hPut h bs

--------------------------------------------------------------------------------

instance (Applicative m, Monad m, MonadIO m) =>
    ConcurrentMetaStoreM FilePath (StoreT FilePath m)
  where
    putConcurrentMeta fp meta = do
        h <- get >>= lookupHandle fp

        let page = ConcurrentMetaPage meta
            bs   = encode page
        liftIO $ hSetFileSize h (fromIntegral $ BS.length bs)
        liftIO $ hSeek h AbsoluteSeek 0
        liftIO $ BS.hPut h bs

    readConcurrentMeta fp k v = do
        fh       <- get >>=  lookupHandle fp
        liftIO $ hSeek fh AbsoluteSeek 0
        bs <- liftIO $ BS.hGetContents fh
        decodeM (concurrentMetaPage k v) bs >>= \case
            ConcurrentMetaPage meta -> return $ Just (coerce meta)

--------------------------------------------------------------------------------
