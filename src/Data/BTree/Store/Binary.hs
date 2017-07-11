{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}

module Data.BTree.Store.Binary where

import Data.BTree.Alloc.Append
import Data.BTree.Primitives
import Data.BTree.Store.Class

import Control.Applicative              (Applicative(..), (<$>))
import Control.Monad
import Control.Monad.Identity
import Control.Monad.State.Class
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.State.Strict ( StateT, evalStateT, execStateT
                                        , runStateT
                                        )

import           Data.Binary (Binary(..), Put, Get)
import qualified Data.Binary as B
import qualified Data.Binary.Get as B
import qualified Data.Binary.Put as B
import           Data.ByteString (ByteString)
import           Data.ByteString.Lazy (fromStrict, toStrict)
import qualified Data.ByteString.Lazy as BL
import           Data.Coerce
import           Data.Map (Map)
import qualified Data.Map as M
import           Data.Proxy
import           Data.Typeable

import GHC.Generics (Generic)

--------------------------------------------------------------------------------

data Page = PageEmpty
    | forall height key val. (Key key, Value val) =>
      PageNode (Height height) (Node height key val)
    | forall key val.        (Key key, Value val) =>
      PageAppendMeta (AppendMeta key val)
    deriving (Typeable)

encode :: Page -> ByteString
encode = toStrict . B.runPut . putPage

decode :: Get a -> ByteString -> a
decode g = B.runGet g . fromStrict

deriving instance Show Page

data BPage = BPageEmpty | BPageNode | BPageAppendMeta
           deriving (Eq, Generic)
instance Binary BPage where

putPage :: Page -> Put
putPage PageEmpty = B.put BPageEmpty
putPage (PageNode h n) = B.put BPageNode >> B.put h >> putNode n
putPage (PageAppendMeta m) = B.put BPageAppendMeta >> B.put m

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

type File     = Map PageId ByteString
type Files fp = Map fp File

newtype StoreT fp m a = StoreT
    { fromStoreT :: MaybeT (StateT (Files fp) m) a
    } deriving (Applicative, Functor, Monad)

runStoreT :: StoreT fp m a -> Files fp -> m (Maybe a, Files fp)
runStoreT = runStateT . runMaybeT . fromStoreT

evalStoreT :: Monad m => StoreT fp m a -> Files fp -> m (Maybe a)
evalStoreT = evalStateT . runMaybeT . fromStoreT

execStoreT :: Monad m => StoreT fp m a -> Files fp-> m (Files fp)
execStoreT = execStateT . runMaybeT . fromStoreT

runStore :: StoreT String Identity a -> (Maybe a, Files String)
runStore = runIdentity . flip runStoreT initialStore
  where initialStore = M.fromList [("Main", M.empty)]

evalStore :: StoreT String Identity a -> Maybe a
evalStore = fst . runStore

nodeIdToPageId :: NodeId height key val -> PageId
nodeIdToPageId (NodeId n) = PageId n

pageIdToNodeId :: PageId -> NodeId height key val
pageIdToNodeId (PageId n) = NodeId n

--------------------------------------------------------------------------------

instance (Ord fp, Applicative m, Monad m) =>
    StoreM fp (StoreT fp m)
  where
    -- -- openStore fp = StoreT $ do
    -- --     modify (M.insertWith (flip const) fp M.empty)
    -- --     return fp
    -- closeStore _ = return ()
    nodePageSize = return $ \h ->
        fromIntegral . BL.length . B.runPut . putPage . PageNode h
    maxPageSize = return 64
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
        PageNode heightSrc tree <- return (decode (getPageNode height key val) bs)
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

--------------------------------------------------------------------------------
