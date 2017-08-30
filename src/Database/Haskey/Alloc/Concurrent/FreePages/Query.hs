{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
module Database.Haskey.Alloc.Concurrent.FreePages.Query where

import Control.Applicative ((<|>), (<$>))
import Control.Concurrent.STM
import Control.Monad.State
import Control.Monad.Trans.Maybe

import Data.List.NonEmpty (NonEmpty((:|)))
import qualified Data.List.NonEmpty as NE

import Data.BTree.Alloc.Class
import Data.BTree.Impure
import Data.BTree.Impure.NonEmpty
import Data.BTree.Primitives

import Database.Haskey.Alloc.Concurrent.Environment
import Database.Haskey.Alloc.Concurrent.FreePages.Tree
import Database.Haskey.Utils.Monad (ifM)
import qualified Database.Haskey.Utils.STM.Map as Map

-- | Get a free page.
--
-- First try to get one from the in-memory dirty pages. Then try to get one
-- from the in-memory free page cache stored in 'fileStateCachedFreePages'. If
-- that one is empty, actually query one from the free database.
getFreePageId :: (Functor m, AllocM m, MonadIO m, MonadState (WriterEnv hnd) m)
              => S stateType ()
              -> m (Maybe PageId)
getFreePageId t =
    runMaybeT $ MaybeT (getCachedFreePageId t)
            <|> MaybeT (queryNewFreePageIds t)

-- | Get a cached free page.
--
-- Get a free page from the free database cache stored in
-- 'fileStateCachedFreePages'.
getCachedFreePageId :: (Functor m, MonadState (WriterEnv hnd) m)
                    => S stateType ()
                    -> m (Maybe PageId)
getCachedFreePageId stateType =
    case stateType of
        DataState () -> do
            s <- writerDataFileState <$> get
            let (pid, s') = query DataState s
            modify' $ \env -> env { writerDataFileState = s' }
            return pid
        IndexState () -> do
            s <- writerIndexFileState <$> get
            let (pid, s') = query IndexState s
            modify' $ \env -> env { writerIndexFileState = s' }
            return pid
  where
    query :: (forall a. a -> S t a)
          -> FileState t
          -> (Maybe PageId, FileState t)
    query cons env = case getSValue $ fileStateCachedFreePages env of
        [] -> (Nothing, env)
        FreePage pid : pageIds ->
            let env' = env { fileStateCachedFreePages = cons pageIds } in
            (Just pid, env')

-- | Try to get a list of free pages from the free page database, return the
-- first free one for immediate use, and store the rest in the environment.
--
-- Immediately remove the queried free pages from the free tree.
queryNewFreePageIds :: (AllocM m, MonadIO m, MonadState (WriterEnv hnd) m)
                 => S stateType ()
                 -> m (Maybe PageId)
queryNewFreePageIds stateType = ifM (not . writerQueryFreeTreeOn <$> get) (return Nothing) $ do
    flag <- case stateType of
        DataState () ->
            query DataState
                  writerDataFileState
                  (\e s -> e { writerDataFileState = s })

        IndexState () ->
            query IndexState
                  writerIndexFileState
                  (\e s -> e { writerIndexFileState = s })

    if flag then getFreePageId stateType
            else return Nothing
  where
    query :: (AllocM m, MonadIO m, MonadState (WriterEnv hnd) m)
          => (forall a. a -> S t a)
          -> (forall h. WriterEnv h -> FileState t)
          -> (forall h. WriterEnv h -> FileState t -> WriterEnv h)
          -> m Bool
    query cons getState setState =  do
        tree <- gets $ getSValue . fileStateFreeTree . getState

        -- Lookup the oldest free page
        lookupValidFreePageIds tree >>= \case
            Nothing -> return False
            Just (txId, x :| xs) -> do
                -- Save them for reuse
                modify' $ \e ->
                    let s    = getState e
                        pids = map FreePage (x:xs)
                    in setState e $
                    s { fileStateCachedFreePages =
                            cons $ pids ++ getSValue (fileStateCachedFreePages s) }

                -- Remove the entry from the tree
                modify' $ \e -> e { writerQueryFreeTreeOn = False }
                tree' <- txId `deleteSubtree` tree
                modify' $ \e -> e { writerQueryFreeTreeOn = True }

                -- Update the tree
                modify' $ \e -> setState e $
                    (getState e) { fileStateFreeTree = cons tree' }

                return True

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
    tx      <- writerTxId <$> get
    if maybe True (> fst v) oldest && fst v + 1 < tx
        then return (Just v)
        else return Nothing
