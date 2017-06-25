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
import           Data.Coerce
import           Data.Map (Map)
import qualified Data.Map as M
import           Data.Typeable

import GHC.Generics (Generic)

--------------------------------------------------------------------------------

data Page key val = PageEmpty
    | forall height . (Key key, Value val, Binary key, Binary val) =>
      PageNode (Height height) (Node height key val)
    | (Key key, Value val) =>
      PageAppendMeta (AppendMeta key val)
    deriving (Typeable)

pageNode :: Page key val -> Maybe (Node height key val)
pageNode = undefined

encode :: (Binary k, Binary v) => Page k v -> ByteString
encode = toStrict . B.encode

decode :: (Binary k, Binary v) => ByteString -> Page k v
decode = B.decode  . fromStrict

deriving instance Show (Page k v)

data BPage = BPageEmpty | BPageNode | BPageAppendMeta
           deriving (Eq, Generic)
instance Binary BPage where

instance (Binary k, Binary v) => Binary (Page k v) where
    put PageEmpty = B.put BPageEmpty
    put (PageNode h n) =
        case castNode' h n of
            Left  n' -> B.put BPageNode >> B.put h >> B.put n'
            Right n' -> B.put BPageNode >> B.put h >> B.put n'
    put (PageAppendMeta m) = B.put BPageAppendMeta >> B.put m

    get = B.get >>= \case
        BPageEmpty      -> pure PageEmpty
        BPageNode       -> undefined -- PageNode <$> B.get <*> B.get
        BPageAppendMeta -> undefined -- PageAppendMeta <$> B.get

--------------------------------------------------------------------------------

type File k v     = Map PageId ByteString
type Files fp k v = Map fp (File k v)

newtype StoreT fp k v m a = StoreT
    { fromStoreT :: MaybeT (StateT (Files fp k v) m) a
    } deriving (Applicative, Functor, Monad)

runStoreT :: StoreT fp k v m a -> Files fp k v -> m (Maybe a, Files fp k v)
runStoreT = runStateT . runMaybeT . fromStoreT

evalStoreT :: Monad m => StoreT fp k v m a -> Files fp k v -> m (Maybe a)
evalStoreT = evalStateT . runMaybeT . fromStoreT

execStoreT :: Monad m => StoreT fp k v m a -> Files fp k v-> m (Files fp k v)
execStoreT = execStateT . runMaybeT . fromStoreT

nodeIdToPageId :: NodeId height key val -> PageId
nodeIdToPageId (NodeId n) = PageId n

pageIdToNodeId :: PageId -> NodeId height key val
pageIdToNodeId (PageId n) = NodeId n

--------------------------------------------------------------------------------

instance (Ord fp, Binary k, Binary v, Applicative m, Monad m) =>
    StoreM fp (StoreT fp k v m)
  where
    -- -- openStore fp = StoreT $ do
    -- --     modify (M.insertWith (flip const) fp M.empty)
    -- --     return fp
    -- closeStore _ = return ()
    setSize fp (PageCount n) = StoreT $ do
        let emptyFile = M.fromList [ (PageId i, encode (PageEmpty :: Page k v)) | i <- [0..n-1] ]
            res file = M.intersection (M.union file emptyFile) emptyFile
        modify (M.update (Just . res) fp)
    getNodePage hnd height nid = StoreT $ do
        Just bs <-
            gets (M.lookup hnd >=> M.lookup (nodeIdToPageId nid))
        PageNode heightSrc tree <- return (decode bs :: Page k v)
        MaybeT $ return (castNode heightSrc height tree)

    putNodePage hnd height nid node = StoreT $
        modify (M.update (Just . M.insert (nodeIdToPageId nid) (encode
                    (PageNode height node))) hnd)
    getSize hnd = StoreT $
        fromIntegral . M.size <$> MaybeT (gets (M.lookup hnd))

--------------------------------------------------------------------------------

instance (Ord fp, Binary k, Binary v, Applicative m, Monad m) =>
    AppendMetaStoreM fp (StoreT fp k v m)
  where
    getAppendMeta h i = StoreT $ do
        Just bs <- gets (M.lookup h >=> M.lookup i)
        PageAppendMeta meta <- return (decode bs :: Page k v)
        return $! coerce meta
    putAppendMeta h i meta = StoreT $
        modify (M.update (Just. M.insert i (encode (PageAppendMeta meta))) h)

--------------------------------------------------------------------------------
