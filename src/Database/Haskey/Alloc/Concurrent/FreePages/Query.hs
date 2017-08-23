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
import qualified Data.Set as S

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
-- from the in-memory free page cache stored in 'writerReusablePages'. If that
-- one is empty, actually query one from the free database.
getFreePageId :: (Functor m, AllocM m, MonadIO m, MonadState (WriterEnv hnd) m)
              => S stateType ()
              -> m (Maybe SomeFreePage)
getFreePageId t =
    runMaybeT $ (DirtyFreePage <$> MaybeT (getFreedDirtyPageId t))
            <|> (OldFreePage   <$> MaybeT (getCachedFreePageId t))
            <|> (OldFreePage   <$> MaybeT (queryNewFreePageIds t))

-- | Get a free'd dirty page.
--
-- Get a free'd dirty page, that is immediately suitable for reuse in the
-- current transaction.
getFreedDirtyPageId :: (Functor m, MonadState (WriterEnv hnd) m)
                    => S stateType ()
                    -> m (Maybe DirtyFree)
getFreedDirtyPageId stateType =
    case stateType of
        DataState () -> do
            s <- writerDataFileState <$> get
            let (pid, s') = query s DataState
            modify' $ \env -> env { writerDataFileState = s' }
            return pid
        IndexState () -> do
            s <- writerIndexFileState <$> get
            let (pid, s') = query s IndexState
            modify' $ \env -> env { writerIndexFileState = s' }
            return pid
  where
    query :: FileState t
          -> (forall a. a -> S t a)
          -> (Maybe DirtyFree, FileState t)
    query env cons =
        case S.minView (getSValue $ fileStateFreedDirtyPages env) of
            Nothing -> (Nothing, env)
            Just (pid, s') ->
                let env' = env { fileStateFreedDirtyPages = cons s' } in
                (Just pid, env')

-- | Get a cached free page.
--
-- Get a free page from the free database cache stored in 'writerReusablePages'.
getCachedFreePageId :: (Functor m, MonadState (WriterEnv hnd) m)
                    => S stateType ()
                    -> m (Maybe OldFree)
getCachedFreePageId stateType =
    ifM (not . writerReusablePagesOn <$> get) (return Nothing) $
    case stateType of
        DataState () -> do
            s <- writerDataFileState <$> get
            let (pid, s') = query s
            modify' $ \env -> env { writerDataFileState = s' }
            return pid
        IndexState () -> do
            s <- writerIndexFileState <$> get
            let (pid, s') = query s
            modify' $ \env -> env { writerIndexFileState = s' }
            return pid
  where
    query :: FileState t -> (Maybe OldFree, FileState t)
    query env = case fileStateReusablePages env of
        [] -> (Nothing, env)
        pid : pageIds ->
            let env' = env { fileStateReusablePages = pageIds } in
            (Just pid, env')

-- | Try to get a list of free pages from the free page database, return the
-- first free one for immediate use, and store the rest in the environment.
--
-- This function will delete the lastly used entry from the free database,
-- query a new one, and then update the free page cache in the state.
--
-- This function only works when 'writerReusablePagesOn' is 'True'.
--
-- This function expects 'writerReusablePages' to be empty.
queryNewFreePageIds :: (AllocM m, MonadIO m, MonadState (WriterEnv hnd) m)
                 => S stateType ()
                 -> m (Maybe OldFree)
queryNewFreePageIds stateType = ifM (not . writerReusablePagesOn <$> get) (return Nothing) $
    case stateType of
        DataState () ->
            query DataState
                  writerDataFileState
                  (\e s -> e { writerDataFileState = s })

        IndexState () ->
            query IndexState
                  writerIndexFileState
                  (\e s -> e { writerIndexFileState = s })
  where
    query :: (AllocM m, MonadIO m, MonadState (WriterEnv hnd) m)
          => (forall a. a -> S t a)
          -> (forall h. WriterEnv h -> FileState t)
          -> (forall h. WriterEnv h -> FileState t -> WriterEnv h)
          -> m (Maybe OldFree)
    query cons getState setState =  do
        tree    <- gets $ getSValue . fileStateFreeTree . getState
        oldTxId <- gets $ fileStateReusablePagesTxId . getState

        -- Delete the previous used 'TxId' from the tree.
        modify' $ \e -> e { writerReusablePagesOn = False }
        tree' <- maybe (return tree) (`deleteSubtree` tree) oldTxId
        modify' $ \e -> e { writerReusablePagesOn = True }

        -- Set the new free tree
        modify' $ \e -> setState e $
            (getState e) { fileStateFreeTree = cons tree' }

        -- Lookup the oldest free page
        lookupValidFreePageIds tree' >>= \case
            Nothing -> do
                modify' $ \e -> setState e $
                    (getState e) { fileStateDirtyReusablePages = S.empty
                                 , fileStateReusablePages = []
                                 , fileStateReusablePagesTxId = Nothing }
                return Nothing
            Just (txId, pid :| pageIds) -> do
                modify' $ \e -> setState e $
                    (getState e) { fileStateDirtyReusablePages = S.empty
                                 , fileStateReusablePages = map OldFree pageIds
                                 , fileStateReusablePagesTxId = Just txId }
                return (Just $ OldFree pid)

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
