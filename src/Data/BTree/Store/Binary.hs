{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
-- | Binary in-memory storage back-end. Can be used as a storage back-end for
-- the append-only page allocator (see "Data.BTree.Alloc").
module Data.BTree.Store.Binary (
  -- * Storage
  Page(..)
, File
, Files
, StoreT
, runStoreT
, evalStoreT
, execStoreT
, emptyStore

  -- * Exceptions
, FileNotFoundError(..)
, PageNotFoundError(..)
, WrongNodeTypeError(..)
, WrongOverflowValueError(..)
) where

import Control.Applicative (Applicative, (<$>))
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.State.Class
import Control.Monad.Trans.State.Strict ( StateT, evalStateT, execStateT , runStateT)

import Data.ByteString (ByteString)
import Data.Coerce
import Data.Map (Map)
import Data.Typeable (Typeable)
import qualified Data.ByteString as BS
import qualified Data.Map as M

import Data.BTree.Alloc.Concurrent
import Data.BTree.Impure.Structures
import Data.BTree.Primitives
import Data.BTree.Store.Class
import Data.BTree.Store.Page
import Data.BTree.Utils.Monad.Catch (justErrM)

--------------------------------------------------------------------------------

-- | A file containing a collection of pages.
type File     = Map PageId ByteString

-- | A collection of 'File's, each associated with a certain @fp@ handle.
type Files fp = Map fp File

lookupFile :: (MonadThrow m, Ord fp, Show fp, Typeable fp)
           => fp -> Files fp -> m File
lookupFile fp m = justErrM (FileNotFoundError fp) $ M.lookup fp m

lookupPage :: (Functor m, MonadThrow m, Ord fp, Show fp, Typeable fp)
           => fp -> PageId -> Files fp -> m ByteString
lookupPage fp pid m = M.lookup pid <$> lookupFile fp m
                  >>= justErrM (PageNotFoundError fp pid)

-- | Monad in which binary storage operations can take place.
--
--  Two important instances are 'StoreM' making it a storage back-end, and
--  'ConcurrentMetaStoreM' making it a storage back-end compatible with the
--  concurrent page allocator.
newtype StoreT fp m a = StoreT
    { fromStoreT :: StateT (Files fp) m a
    } deriving (Applicative, Functor, Monad,
                MonadIO, MonadThrow, MonadCatch, MonadMask,
                MonadState (Files fp))

-- | Run the storage operations in the 'StoreT' monad, given a collection of
-- 'File's.
runStoreT :: StoreT fp m a -> Files fp -> m (a, Files fp)
runStoreT = runStateT . fromStoreT

-- | Evaluate the storage operations in the 'StoreT' monad, given a colletion
-- of 'File's.
evalStoreT :: Monad m => StoreT fp m a -> Files fp -> m a
evalStoreT = evalStateT . fromStoreT

-- | Execute the storage operations in the 'StoreT' monad, given a colletion of
-- 'File's.
execStoreT :: Monad m => StoreT fp m a -> Files fp-> m (Files fp)
execStoreT = execStateT . fromStoreT

-- | Construct a store with an empty database with name of type @hnd@.
emptyStore :: Files hnd
emptyStore = M.empty

--------------------------------------------------------------------------------

instance (Applicative m, Monad m, MonadThrow m,
          Ord fp, Show fp, Typeable fp) =>
    StoreM fp (StoreT fp m)
  where
    openHandle fp =
        modify $ M.insertWith (flip const) fp M.empty

    closeHandle _ = return ()

    removeHandle fp =
        modify $ M.delete fp

    nodePageSize = return $ \h -> case viewHeight h of
        UZero -> fromIntegral . BS.length . encode . LeafNodePage h
        USucc _ -> fromIntegral . BS.length . encode . IndexNodePage h

    maxPageSize = return 256

    getNodePage hnd h key val nid = do
        bs <- get >>= lookupPage hnd (nodeIdToPageId nid)
        case viewHeight h of
            UZero -> decodeM (leafNodePage h key val) bs >>= \case
                LeafNodePage heightSrc n ->
                    justErrM WrongNodeTypeError $ castNode heightSrc h n
            USucc _ -> decodeM (indexNodePage h key val) bs >>= \case
                IndexNodePage heightSrc n ->
                    justErrM WrongNodeTypeError $ castNode heightSrc h n

    putNodePage hnd height nid node =
        modify $ M.update (Just . M.insert (nodeIdToPageId nid) pg) hnd
      where
        pg = case viewHeight height of
            UZero -> encode $ LeafNodePage height node
            USucc _ -> encode $ IndexNodePage height node

    getOverflow hnd val = do
        bs <- get >>= lookupPage hnd 0
        decodeM (overflowPage val) bs >>= \case
            OverflowPage v -> justErrM WrongOverflowValueError $ castValue v

    putOverflow hnd val =
        modify $ M.update (Just . M.insert 0 pg) hnd
      where
        pg = encode $ OverflowPage val

    listOverflows _ = gets M.keys

--------------------------------------------------------------------------------

instance (Applicative m, Monad m, MonadThrow m) =>
    ConcurrentMetaStoreM (StoreT FilePath m)
  where
    putConcurrentMeta h meta =
        modify $ M.update (Just . M.insert 0 pg) h
      where
        pg = encode $ ConcurrentMetaPage meta

    readConcurrentMeta hnd k v = do
        Just bs <- StoreT $ gets (M.lookup hnd >=> M.lookup 0)
        decodeM (concurrentMetaPage k v) bs >>= \case
            ConcurrentMetaPage meta -> return . Just $! coerce meta

--------------------------------------------------------------------------------

-- | Exception thrown when a file is accessed that doesn't exist.
newtype FileNotFoundError hnd = FileNotFoundError hnd deriving (Show, Typeable)

instance (Typeable hnd, Show hnd) => Exception (FileNotFoundError hnd) where

-- | Exception thrown when a page that is accessed doesn't exist.
data PageNotFoundError hnd = PageNotFoundError hnd PageId deriving (Show, Typeable)

instance (Typeable hnd, Show hnd) => Exception (PageNotFoundError hnd) where

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

