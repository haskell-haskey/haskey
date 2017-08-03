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
) where

import Control.Applicative (Applicative, (<$>))
import Control.Monad
import Control.Monad.Except
import Control.Monad.State.Class
import Control.Monad.Trans.State.Strict ( StateT, evalStateT, execStateT , runStateT)

import Data.Binary (Get)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (fromStrict, toStrict)
import Data.Coerce
import Data.Map (Map)
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map as M

import Data.BTree.Alloc.Concurrent
import Data.BTree.Impure.Structures
import Data.BTree.Primitives
import Data.BTree.Store.Class
import Data.BTree.Store.Page
import Data.BTree.Utils.Monad.Except (justErrM)

--------------------------------------------------------------------------------

-- | A file containing a collection of pages.
type File     = Map PageId ByteString

-- | A collection of 'File's, each associated with a certain @fp@ handle.
type Files fp = Map fp File

lookupFile :: (Ord fp, Show fp, MonadError String m)
           => fp -> Files fp -> m File
lookupFile fp m = justErrM ("no file for handle " ++ show fp) $ M.lookup fp m

lookupPage :: (Ord fp, Show fp, Functor m, MonadError String m)
           => fp -> PageId -> Files fp -> m ByteString
lookupPage fp pid m = M.lookup pid <$> lookupFile fp m
                  >>= justErrM ("no page " ++ show pid ++
                                " for handle " ++ show fp)

-- | Monad in which binary storage operations can take place.
--
--  Two important instances are 'StoreM' making it a storage back-end, and
--  'ConcurrentMetaStoreM' making it a storage back-end compatible with the
--  concurrent page allocator.
newtype StoreT fp m a = StoreT
    { fromStoreT :: ExceptT String (StateT (Files fp) m) a
    } deriving (Applicative, Functor, Monad, MonadIO, MonadState (Files fp), MonadError String)

-- | Run the storage operations in the 'StoreT' monad, given a collection of
-- 'File's.
runStoreT :: StoreT fp m a -> Files fp -> m (Either String a, Files fp)
runStoreT = runStateT . runExceptT . fromStoreT

-- | Evaluate the storage operations in the 'StoreT' monad, given a colletion
-- of 'File's.
evalStoreT :: Monad m => StoreT fp m a -> Files fp -> m (Either String a)
evalStoreT = evalStateT . runExceptT . fromStoreT

-- | Execute the storage operations in the 'StoreT' monad, given a colletion of
-- 'File's.
execStoreT :: Monad m => StoreT fp m a -> Files fp-> m (Files fp)
execStoreT = execStateT . runExceptT . fromStoreT

-- | Construct a store with an empty database with name of type @hnd@.
emptyStore :: Files hnd
emptyStore = M.empty

--------------------------------------------------------------------------------

instance (Show fp, Ord fp, Applicative m, Monad m) =>
    StoreM fp (StoreT fp m)
  where
    openHandle fp =
        modify $ M.insertWith (flip const) fp M.empty

    closeHandle fp =
        modify $ M.delete fp

    nodePageSize = return $ \h ->
        fromIntegral . BL.length . encode . NodePage h

    maxPageSize = return 4096

    setSize fp (PageCount n) = do
        let emptyFile = M.fromList
                        [ (PageId i, encodeStrict EmptyPage)
                        | i <- [0..n-1]
                        ]
            res file  = M.intersection (M.union file emptyFile) emptyFile
        modify (M.update (Just . res) fp)

    getNodePage hnd h key val nid = do
        bs <- get >>= lookupPage hnd (nodeIdToPageId nid)
        decodeStrictM (nodePage h key val) bs >>= \case
            NodePage heightSrc n ->
                justErrM "could not cast node page" $
                    castNode heightSrc h n

    putNodePage hnd height nid node =
        modify $ M.update (Just . M.insert (nodeIdToPageId nid) pg) hnd
      where
        pg = encodeStrict $ NodePage height node

    getSize hnd = do
        m <- get >>= lookupFile hnd
        return $ fromIntegral (M.size m)

--------------------------------------------------------------------------------

instance (Ord fp, Show fp, Applicative m, Monad m) =>
    ConcurrentMetaStoreM fp (StoreT fp m)
  where
    putConcurrentMeta h meta =
        modify $ M.update (Just . M.insert 0 pg) h
      where
        pg = encodeStrict $ ConcurrentMetaPage meta

    readConcurrentMeta hnd k v = do
        Just bs <- StoreT $ gets (M.lookup hnd >=> M.lookup 0)
        case decodeStrictM (concurrentMetaPage k v) bs of
            Right (ConcurrentMetaPage meta) -> return . Just $! coerce meta
            Left err -> throwError err

--------------------------------------------------------------------------------

encodeStrict :: Page t -> ByteString
encodeStrict = toStrict . encode

decodeStrictM :: MonadError String m => Get a -> ByteString -> m a
decodeStrictM g = decodeM g . fromStrict

--------------------------------------------------------------------------------
