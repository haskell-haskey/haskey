{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
module Data.BTree.Alloc.Concurrent.FreePages.Query where

import Control.Applicative ((<|>), (<$>))
import Control.Concurrent.STM
import Control.Monad.State
import Control.Monad.Trans.Maybe

import Data.List.NonEmpty (NonEmpty((:|)))
import qualified Data.List.NonEmpty as NE

import Data.BTree.Alloc.Class
import Data.BTree.Alloc.Concurrent.Environment
import Data.BTree.Alloc.Concurrent.FreePages.Tree
import Data.BTree.Impure
import Data.BTree.Impure.NonEmpty
import Data.BTree.Primitives
import Data.BTree.Utils.Monad (ifM)
import qualified Data.BTree.Utils.STM.Map as Map

-- | Get a free node id.
--
-- Just a convenience wrapper around 'getFreePageId'.
getFreeNodeId :: (AllocM m, MonadIO m, MonadState (WriterEnv hnd) m)
              => m (Maybe (NodeId height k v))
getFreeNodeId = runMaybeT $ pageIdToNodeId <$> MaybeT getFreePageId

-- | Get a free page.
--
-- First try to get one from the in-memory dirty pages. Then try to get one
-- from the in-memory free page cache stored in 'writerReuseablePages'. If that
-- one is empty, actually query one from the free database.
getFreePageId :: (AllocM m, MonadIO m, MonadState (WriterEnv hnd) m)
              => m (Maybe PageId)
getFreePageId = runMaybeT $ MaybeT getFreedDirtyPageId
                        <|> MaybeT getCachedFreePageId
                        <|> MaybeT queryNewFreePageIds

-- | Get a free'd dirty page.
--
-- Get a free'd dirty page, that is immediately suitable for reuse in the
-- current transaction.
getFreedDirtyPageId :: (Functor m, MonadState (WriterEnv hnd) m)
                    => m (Maybe PageId)
getFreedDirtyPageId = writerFreedDirtyPages <$> get >>= \case
    []                      -> return Nothing
    DirtyFree pid : pageIds -> do
        modify' $ \env -> env { writerFreedDirtyPages = pageIds }
        return (Just pid)

-- | Get a cached free page.
--
-- Get a free page from the free database cache stored in 'writerReuseablePages'.
getCachedFreePageId :: (Functor m, MonadState (WriterEnv hnd) m)
                    => m (Maybe PageId)
getCachedFreePageId = writerReuseablePages <$> get >>= \case
    []                 -> return Nothing
    Free pid : pageIds -> do
        modify' $ \env -> env { writerReuseablePages = pageIds }
        return (Just pid)

-- | Try to get a list of free pages from the free page database, return the
-- first free one for immediate use, and store the rest in the environment.
--
-- This function will delete the lastly used entry from the free database,
-- query a new one, and then update the free page cache in the state.
--
-- This function only works when 'writerReusablePagesOn' is 'True'.
--
-- This function expects 'writerReuseablePages' to be empty.
queryNewFreePageIds :: (AllocM m, MonadIO m, MonadState (WriterEnv hnd) m)
                 => m (Maybe PageId)
queryNewFreePageIds = ifM (not . writerReusablePagesOn <$> get) (return Nothing) $ do
    tree    <- writerFreeTree <$> get
    oldTxId <- writerReuseablePagesTxId <$> get

    -- Delete the previous used 'TxId' from the tree.
    modify' $ \env -> env { writerReusablePagesOn = False }
    tree' <- maybe (return tree) (`deleteSubtree` tree) oldTxId
    modify' $ \env -> env { writerReusablePagesOn = True }

    -- Set the new free tree
    modify' $ \env -> env { writerFreeTree = tree' }

    -- Lookup the oldest free page
    lookupValidFreePageIds tree' >>= \case
        Nothing -> do
            modify' $ \env -> env { writerReuseablePages = []
                                  , writerReuseablePagesTxId = Nothing }
            return Nothing
        Just (txId, pid :| pageIds) -> do
            modify' $ \ env -> env { writerReuseablePages = map Free pageIds
                                   , writerReuseablePagesTxId = Just txId }
            return (Just pid)

-- | Lookup a list of free pages from the free page database, guaranteed to be old enough.
lookupValidFreePageIds :: (MonadIO m, AllocReaderM m, MonadState (WriterEnv hnd) m)
                       => FreeTree
                       -> m (Maybe (TxId, NonEmpty PageId))
lookupValidFreePageIds tree = runMaybeT $
    MaybeT (lookupFreePageIds tree) >>= (MaybeT . checkFreePages)

-- | Lookup a list of free pages from the free page database.
lookupFreePageIds :: (Functor m, AllocReaderM m, MonadState (WriterEnv hnd) m)
                  => FreeTree
                  -> m (Maybe (Unchecked (TxId, NonEmpty PageId)))
lookupFreePageIds tree = lookupMinTree tree >>= \case
    Nothing -> return Nothing
    Just (tx, subtree) -> do
        pids <- subtreeToList subtree
        return . Just $ Unchecked (tx, pids)
  where
    subtreeToList subtree = NE.map fst <$> nonEmptyToList subtree

-- | Auxiliry type to ensure the transaction ID of free pages are checked.
newtype Unchecked a = Unchecked a

-- | Check the transaction ID of the free pages, if it's to old, return
-- 'Nothing'.
checkFreePages :: (Functor m, MonadIO m, MonadState (WriterEnv hnd) m)
              => Unchecked (TxId, NonEmpty PageId)
              -> m (Maybe (TxId, NonEmpty PageId))
checkFreePages (Unchecked v) = do
    readers <- writerReaders <$> get
    oldest  <- liftIO . atomically $ Map.lookupMinKey readers
    if maybe True (> fst v) oldest
        then return (Just v)
        else return Nothing
