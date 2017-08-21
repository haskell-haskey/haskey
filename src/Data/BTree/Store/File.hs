{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
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

  -- * Exceptions
, FileNotFoundError(..)
, PageOverflowError(..)
, WrongNodeTypeError(..)
, WrongOverflowValueError(..)
) where

import Control.Applicative (Applicative, (<$>))
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.State.Class
import Control.Monad.Trans.State.Strict ( StateT, evalStateT, execStateT , runStateT)

import Data.Coerce (coerce)
import Data.Map (Map)
import Data.Monoid ((<>))
import Data.Typeable (Typeable)
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map as M

import qualified FileIO as IO

import System.Directory (createDirectoryIfMissing, removeFile, getDirectoryContents)
import System.FilePath (takeDirectory)
import System.IO.Error (ioError, isDoesNotExistError)

import Data.BTree.Alloc.Concurrent
import Data.BTree.Impure.Structures

import Data.BTree.Primitives
import Data.BTree.Store.Class
import Data.BTree.Store.Page
import Data.BTree.Utils.IO (readByteString, writeLazyByteString)
import Data.BTree.Utils.Monad.Catch (justErrM)

--------------------------------------------------------------------------------

-- | Encode a page padding it to the maxim page size.
--
-- Return 'Nothing' of the page is too large to fit into one page size.
encodeAndPad :: PageSize -> Page t -> Maybe BL.ByteString
encodeAndPad size page
    | Just n <- padding = Just . prependChecksum $
        enc <> BL.replicate n 0
    | otherwise = Nothing
  where
    enc = encodeNoChecksum page

    -- Reserve 4 bytes for the checksum
    padding | n <- fromIntegral size - BL.length enc - 4, n >= 0 = Just n
            | otherwise = Nothing

--------------------------------------------------------------------------------

-- | A collection of files, each associated with a certain @fp@ handle.
--
-- Each file is a 'Handle' opened in 'System.IO.ReadWriteMode' and contains a
-- collection of physical pages.
type Files fp = Map fp IO.FHandle


lookupHandle :: (Functor m, MonadThrow m, Ord fp, Show fp, Typeable fp)
             => fp -> Files fp -> m IO.FHandle
lookupHandle fp m = justErrM (FileNotFoundError fp) $ M.lookup fp m

-- | Monad in which on-disk storage operations can take place.
--
-- Two important instances are 'StoreM' making it a storage back-end, and
-- 'ConcurrentMetaStoreM' making it a storage back-end compatible with the
-- concurrent page allocator.
newtype StoreT fp m a = StoreT
    { fromStoreT :: StateT (Files fp) m a
    } deriving (Applicative, Functor, Monad,
                MonadIO, MonadThrow, MonadCatch, MonadMask,
                MonadState (Files fp))

-- | Run the storage operations in the 'StoreT' monad, given a collection of
-- open files.
runStoreT :: StoreT fp m a -> Files fp -> m (a, Files fp)
runStoreT = runStateT . fromStoreT

-- | Evaluate the storage operations in the 'StoreT' monad, given a collection
-- of open files.
evalStoreT :: Monad m => StoreT fp m a -> Files fp -> m a
evalStoreT = evalStateT . fromStoreT

-- | Execute the storage operations in the 'StoreT' monad, given a collection
-- of open files.
execStoreT :: Monad m => StoreT fp m a -> Files fp -> m (Files fp)
execStoreT = execStateT . fromStoreT

-- | An empty file store, with no open files.
emptyStore :: Files fp
emptyStore = M.empty

--------------------------------------------------------------------------------

instance (Applicative m, Monad m, MonadIO m, MonadThrow m) =>
    StoreM FilePath (StoreT FilePath m)
  where
    openHandle fp = do
        alreadyOpen <- M.member fp <$> get
        unless alreadyOpen $ do
            liftIO $ createDirectoryIfMissing True (takeDirectory fp)
            fh <- liftIO $ IO.openReadWrite fp
            modify $ M.insert fp fh

    closeHandle fp = do
        fh <- get >>= lookupHandle fp
        liftIO $ IO.flush fh
        liftIO $ IO.close fh
        modify (M.delete fp)

    removeHandle fp =
        liftIO $ removeFile fp `catchIOError` \e ->
            unless (isDoesNotExistError e) (ioError e)

    nodePageSize = return $ \h -> case viewHeight h of
        UZero -> fromIntegral . BL.length . encodeZeroChecksum . LeafNodePage h
        USucc _ -> fromIntegral . BL.length . encodeZeroChecksum . IndexNodePage h

    maxPageSize = return 256

    getNodePage fp height key val nid = do
        h    <- get >>= lookupHandle fp
        size <- maxPageSize

        let PageId pid = nodeIdToPageId nid
            offset     = fromIntegral $ pid * fromIntegral size

        liftIO $ IO.seek h offset
        bs <- liftIO $ readByteString h (fromIntegral size)

        case viewHeight height of
            UZero -> decodeM (leafNodePage height key val) bs >>= \case
                LeafNodePage hgtSrc tree ->
                    justErrM WrongNodeTypeError $ castNode hgtSrc height tree
            USucc _ -> decodeM (indexNodePage height key val) bs >>= \case
                IndexNodePage hgtSrc tree ->
                    justErrM WrongNodeTypeError $ castNode hgtSrc height tree

    putNodePage fp hgt nid node = do
        h    <- get >>= lookupHandle fp
        size <- maxPageSize

        let PageId pid = nodeIdToPageId nid
            offset     = fromIntegral $ pid * fromIntegral size

        liftIO $ IO.seek h offset
        bs <- justErrM PageOverflowError $ pg size
        liftIO $ writeLazyByteString h bs
      where
        pg size = case viewHeight hgt of
            UZero -> encodeAndPad size $ LeafNodePage hgt node
            USucc _ -> encodeAndPad size $ IndexNodePage hgt node

    getOverflow fp val = do
        h <- get >>= lookupHandle fp

        len <- liftIO $ IO.getFileSize h
        liftIO $ IO.seek h 0
        bs <- liftIO $ readByteString h (fromIntegral len)
        n <- decodeM (overflowPage val) bs
        case n of
            OverflowPage v -> justErrM WrongOverflowValueError $ castValue v


    putOverflow fp val = do
        fh <- get >>= lookupHandle fp
        liftIO $ IO.setFileSize fh (fromIntegral $ BL.length bs)
        liftIO $ IO.seek fh 0
        liftIO $ writeLazyByteString fh bs
      where
        bs = encode $ OverflowPage val

    listOverflows dir = liftIO $ getDirectoryContents dir `catch` catch'
      where catch' e | isDoesNotExistError e = return []
                     | otherwise = ioError e

--------------------------------------------------------------------------------

instance (Applicative m, Monad m, MonadIO m, MonadThrow m) =>
    ConcurrentMetaStoreM (StoreT FilePath m)
  where
    putConcurrentMeta fp meta = do
        h <- get >>= lookupHandle fp

        let page = ConcurrentMetaPage meta
            bs   = encode page
        liftIO $ IO.setFileSize h (fromIntegral $ BL.length bs)
        liftIO $ IO.seek h 0
        liftIO $ writeLazyByteString h bs

    readConcurrentMeta fp k v = do
        fh <- get >>=  lookupHandle fp

        len <- liftIO $ IO.getFileSize fh
        liftIO $ IO.seek fh 0
        bs <- liftIO $ readByteString fh (fromIntegral len)
        decodeM (concurrentMetaPage k v) bs >>= \case
            ConcurrentMetaPage meta -> return $ Just (coerce meta)

--------------------------------------------------------------------------------

-- | Exception thrown when a file is accessed that doesn't exist.
newtype FileNotFoundError hnd = FileNotFoundError hnd deriving (Show, Typeable)

instance (Typeable hnd, Show hnd) => Exception (FileNotFoundError hnd) where

-- | Exception thrown when a page that is too large is written.
--
-- As used in 'putNodePage'.
data PageOverflowError = PageOverflowError deriving (Show, Typeable)

instance Exception PageOverflowError where

-- | Exception thrown when a node cannot be cast to the right type.
--
-- As used in 'getNodePage'.
data WrongNodeTypeError = WrongNodeTypeError deriving (Show, Typeable)

instance Exception WrongNodeTypeError where

-- | Exception thrown when a value from an overflow page cannot be cast.
--
-- As used in 'getOverflow'.
data WrongOverflowValueError = WrongOverflowValueError deriving (Show, Typeable)

instance Exception WrongOverflowValueError where

--------------------------------------------------------------------------------
