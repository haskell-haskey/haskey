{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE StandaloneDeriving #-}

module Data.BTree.Store.Debug where

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

import           Data.Coerce
import           Data.Map (Map)
import qualified Data.Map as M
import           Data.Typeable

--------------------------------------------------------------------------------

data Page
    = PageEmpty
    | forall height key val. (Key key, Value val) =>
      PageNode (Height height) (Node height key val)
    | forall key val.        (Key key, Value val) =>
      PageAppendMeta (AppendMeta key val)
    deriving (Typeable)

deriving instance Show Page

--------------------------------------------------------------------------------

type File     = Map PageId Page
type Files fp = Map fp File

newtype StoreT fp m a = StoreT
    { fromStoreT :: MaybeT (StateT (Files fp) m) a
    } deriving (Applicative, Functor, Monad)

runStoreT :: StoreT fp m a -> Files fp -> m (Maybe a, Files fp)
runStoreT = runStateT . runMaybeT . fromStoreT

evalStoreT :: Monad m => StoreT fp m a -> Files fp -> m (Maybe a)
evalStoreT = evalStateT . runMaybeT . fromStoreT

execStoreT :: Monad m => StoreT fp m a -> Files fp -> m (Files fp)
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

instance (Ord fp, Applicative m, Monad m) => StoreM fp (StoreT fp m) where
    -- -- openStore fp = StoreT $ do
    -- --     modify (M.insertWith (flip const) fp M.empty)
    -- --     return fp
    -- closeStore _ = return ()
    maxPageSize = return 64
    nodePageSize = undefined

    setSize fp (PageCount n) = StoreT $ do
        let emptyFile = M.fromList [ (PageId i,PageEmpty) | i <- [0..n-1] ]
            res :: File -> File
            res file = M.intersection (M.union file emptyFile) emptyFile
        modify (M.update (Just . res) fp)
    getNodePage hnd heightDst nid = StoreT $ do
        Just (PageNode heightSrc tree) <-
            gets (M.lookup hnd >=> M.lookup (nodeIdToPageId nid))
        MaybeT (return (castNode heightSrc heightDst tree))
    putNodePage hnd height nid node = StoreT $
        modify (M.update (Just . M.insert (nodeIdToPageId nid)
                  (PageNode height node)) hnd)
    getSize hnd = StoreT $
        fromIntegral . M.size <$> MaybeT (gets (M.lookup hnd))

--------------------------------------------------------------------------------

instance (Ord fp, Applicative m, Monad m) =>
    AppendMetaStoreM fp (StoreT fp m)
  where
    getAppendMeta h i = StoreT $ do
        Just (PageAppendMeta meta) <- gets (M.lookup h >=> M.lookup i)
        return $! coerce meta
    putAppendMeta h i meta = StoreT $
        modify (M.update (Just . M.insert i (PageAppendMeta meta)) h)

--------------------------------------------------------------------------------
