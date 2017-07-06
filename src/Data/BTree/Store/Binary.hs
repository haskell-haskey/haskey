{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}

module Data.BTree.Store.Binary where

import Data.BTree.Alloc.Append
import Data.BTree.Primitives
import Data.BTree.Store.Class

import Control.Applicative              (Applicative(..), (<$>))
import Control.Monad
import Control.Monad.State.Class
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.State.Strict ( StateT, evalStateT, execStateT
                                        , runStateT
                                        )

import           Data.Binary (Binary(..))
import qualified Data.Binary as B
import           Data.ByteString (ByteString)
import           Data.ByteString.Lazy (fromStrict, toStrict)
import qualified Data.ByteString.Lazy as BL
import           Data.Coerce
import           Data.Map (Map)
import qualified Data.Map as M
import           Data.Typeable

import GHC.Generics (Generic)

--------------------------------------------------------------------------------

data Page = PageEmpty
    | forall height key val. (Key key, Value val) =>
      PageNode (Height height) (Node height key val)
    | forall key val.        (Key key, Value val) =>
      PageAppendMeta (AppendMeta key val)
    deriving (Typeable)

encode :: Binary a => a -> ByteString
encode = toStrict . B.encode

decode :: Binary a => ByteString -> a
decode = B.decode  . fromStrict

deriving instance Show Page

data BPage = BPageEmpty | BPageNode | BPageAppendMeta
           deriving (Eq, Generic)
instance Binary BPage where

instance Binary Page where
    put PageEmpty = B.put BPageEmpty
    put (PageNode h n) = B.put BPageNode >> B.put h >> putNode n
    put (PageAppendMeta m) = B.put BPageAppendMeta >> B.put m

    get = B.get >>= \case
        BPageEmpty      -> pure PageEmpty
        BPageNode       -> undefined -- PageNode <$> B.get <*> B.get
        BPageAppendMeta -> undefined -- PageAppendMeta <$> B.get

--------------------------------------------------------------------------------

type File     = Map PageId ByteString
type Files fp = Map fp File

newtype StoreT fp k v m a = StoreT
    { fromStoreT :: MaybeT (StateT (Files fp) m) a
    } deriving (Applicative, Functor, Monad)

runStoreT :: StoreT fp k v m a -> Files fp -> m (Maybe a, Files fp)
runStoreT = runStateT . runMaybeT . fromStoreT

evalStoreT :: Monad m => StoreT fp k v m a -> Files fp -> m (Maybe a)
evalStoreT = evalStateT . runMaybeT . fromStoreT

execStoreT :: Monad m => StoreT fp k v m a -> Files fp-> m (Files fp)
execStoreT = execStateT . runMaybeT . fromStoreT

nodeIdToPageId :: NodeId height key val -> PageId
nodeIdToPageId (NodeId n) = PageId n

pageIdToNodeId :: PageId -> NodeId height key val
pageIdToNodeId (PageId n) = NodeId n

--------------------------------------------------------------------------------

instance (Ord fp, Applicative m, Monad m) =>
    StoreM fp (StoreT fp k v m)
  where
    -- -- openStore fp = StoreT $ do
    -- --     modify (M.insertWith (flip const) fp M.empty)
    -- --     return fp
    -- closeStore _ = return ()
    nodePageSize = return $ \h ->
        fromIntegral . BL.length . B.encode . PageNode h
    maxPageSize = return 64
    setSize fp (PageCount n) = StoreT $ do
        let emptyFile = M.fromList
                        [ (PageId i, encode PageEmpty)
                        | i <- [0..n-1]
                        ]
            res file  = M.intersection (M.union file emptyFile) emptyFile
        modify (M.update (Just . res) fp)
    getNodePage hnd height nid = StoreT $ do
        Just bs <-
            gets (M.lookup hnd >=> M.lookup (nodeIdToPageId nid))
        PageNode heightSrc tree <- return (decode bs)
        MaybeT $ return (castNode heightSrc height tree)

    putNodePage hnd height nid node = StoreT $
        modify (M.update (Just . M.insert (nodeIdToPageId nid) (encode
                    (PageNode height node))) hnd)
    getSize hnd = StoreT $
        fromIntegral . M.size <$> MaybeT (gets (M.lookup hnd))

--------------------------------------------------------------------------------

instance (Ord fp, Applicative m, Monad m) =>
    AppendMetaStoreM fp (StoreT fp k v m)
  where
    getAppendMeta h i = StoreT $ do
        Just bs <- gets (M.lookup h >=> M.lookup i)
        PageAppendMeta meta <- return (decode bs)
        return $! coerce meta
    putAppendMeta h i meta = StoreT $
        modify (M.update (Just. M.insert i (encode (PageAppendMeta meta))) h)

--------------------------------------------------------------------------------
