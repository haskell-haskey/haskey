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

import Data.ByteString.Lazy (ByteString)
import Data.Coerce (coerce)
import Data.Map (Map)
import Data.Monoid ((<>))
import qualified Data.ByteString.Lazy as BL
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
        enc <> BL.replicate n 0
    | otherwise = Nothing
  where
    enc = encode page
    padding | n <- fromIntegral size - BL.length enc, n >= 0 = Just n
            | otherwise = Nothing

--------------------------------------------------------------------------------

-- | A collection of files, each associated with a certain @fp@ handle.
--
-- Each file is a 'Handle' opened in 'System.IO.ReadWriteMode' and contains a
-- collection of physical pages.
type Files fp = Map fp (Handle, PageCount)

getFileHandle :: (Handle, PageCount) -> Handle
getFileHandle = fst

getFilePageCount :: (Handle, PageCount) -> PageCount
getFilePageCount = snd

lookupFile :: (Ord fp, Show fp, MonadError String m) => fp -> Files fp -> m (Handle, PageCount)
lookupFile fp m = justErrM ("no file for handle " ++ show fp) $ M.lookup fp m

lookupHandle :: (Ord fp, Show fp, MonadError String m) => fp -> Files fp -> m Handle
lookupHandle fp m = getFileHandle <$> lookupFile fp m

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
        pageSize    <- maxPageSize
        unless alreadyOpen $ do
            -- Open the file in rw mode
            fh <- liftIO $ openFile fp ReadWriteMode

            -- Calculate the number of pages
            fs     <- liftIO $ hFileSize fh
            let pc = fs `quot` fromIntegral pageSize
            modify $ M.insert fp (fh, fromIntegral pc)

    closeHandle fp = do
        fh <- get >>= lookupHandle fp
        liftIO $ hClose fh
        modify (M.delete fp)

    nodePageSize =
        return $ \h ->
            fromIntegral . BL.length . encode . NodePage h

    maxPageSize = return 4096

    getSize fp = do
        f <- get >>= lookupFile fp
        return (getFilePageCount f)

    setSize fp pc = do
        h <- get >>= lookupHandle fp
        modify (M.insert fp (h, pc))

    getNodePage fp height key val nid = do
        h    <- get >>= lookupHandle fp
        size <- maxPageSize

        let PageId pid = nodeIdToPageId nid
            offset     = fromIntegral $ pid * fromIntegral size

        liftIO $ hSeek h AbsoluteSeek offset
        bs <- liftIO $ BL.hGet h (fromIntegral size)
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
        liftIO $ BL.hPut h bs

--------------------------------------------------------------------------------

instance (Applicative m, Monad m, MonadIO m) =>
    ConcurrentMetaStoreM FilePath (StoreT FilePath m)
  where
    putConcurrentMeta fp meta = do
        h <- get >>= lookupHandle fp

        let page = ConcurrentMetaPage meta
            bs   = encode page
        liftIO $ hSetFileSize h (fromIntegral $ BL.length bs)
        liftIO $ hSeek h AbsoluteSeek 0
        liftIO $ BL.hPut h bs

    readConcurrentMeta fp k v = do
        fh       <- get >>=  lookupHandle fp
        liftIO $ hSeek fh AbsoluteSeek 0
        bs <- liftIO $ BL.hGetContents fh
        decodeM (concurrentMetaPage k v) bs >>= \case
            ConcurrentMetaPage meta -> return $ Just (coerce meta)

--------------------------------------------------------------------------------
