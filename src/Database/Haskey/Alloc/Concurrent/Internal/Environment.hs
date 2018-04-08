{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE StandaloneDeriving #-}
-- | Environments of a read or write transaction.
module Database.Haskey.Alloc.Concurrent.Internal.Environment where

import Control.Applicative ((<$>))
import Control.Monad.State

import Data.Binary (Binary)
import Data.Set (Set)
import Data.Typeable (Typeable)
import Data.Word (Word32)
import qualified Data.Binary as B
import qualified Data.Set as S

import STMContainers.Map (Map)

import Data.BTree.Primitives

import Database.Haskey.Alloc.Concurrent.Internal.FreePages.Tree

data StateType = TypeData
               | TypeIndex

-- | Wrapper around a type to indicate it belongs to a file with either
-- data/leaf nodes or index nodes.
data S (t :: StateType) a where
    DataState  :: a -> S 'TypeData  a
    IndexState :: a -> S 'TypeIndex a
    deriving (Typeable)

deriving instance Show a => Show (S t a)

instance Binary a => Binary (S 'TypeData a) where
    put (DataState a) = B.put a
    get = DataState <$> B.get

instance Binary a => Binary (S 'TypeIndex a) where
    put (IndexState a) = B.put a
    get = IndexState <$> B.get

instance Functor (S t) where
    f `fmap` (DataState v) = DataState (f v)
    f `fmap` (IndexState v) = IndexState (f v)

getSValue :: S t a -> a
getSValue (DataState a)  = a
getSValue (IndexState a) = a

newtype ReaderEnv hnds = ReaderEnv { readerHnds :: hnds }

data FileState stateType = FileState {
      fileStateNewlyFreedPages :: ![NewlyFreed]
    -- ^ Pages free'd in this transaction, not ready for reuse until the
    -- transaction is commited.

    , fileStateOriginalNumPages :: !(S stateType PageId)
    -- ^ The original number of pages in the file, before the transaction
    -- started.

    , fileStateNewNumPages :: !(S stateType PageId)
    -- ^ The new uncommited number of pages in the file.
    --
    -- All pages in the range 'fileStateOriginalNumPages' to
    -- 'fileStateNewNumPages' (excluding) are freshly allocated in the
    -- ongoing transaction.

    , fileStateDirtyPages :: !(Set PageId)
    -- ^ Pages written to in this transaction.

    , fileStateFreeTree :: !(S stateType FreeTree)
    -- ^ The root of the free tree, might change during a transaction.

    , fileStateCachedFreePages :: !(S stateType [FreePage])
    -- ^ All pages that are immediately ready for reuse in this and any
    -- subsequent transactions.
    }

data WriterEnv hnds = WriterEnv
    { writerHnds :: !hnds
    , writerTxId :: !TxId
    , writerReaders :: Map TxId Integer

    , writerIndexFileState :: FileState 'TypeIndex
    -- ^ State of the file with index nodes.

    , writerDataFileState :: FileState 'TypeData
    -- ^ State of the file with data/leaf nodes.

    , writerQueryFreeTreeOn :: !Bool
    -- ^ Whether or not querying free pages from the free is enabled.

    , writerDirtyOverflows :: !(Set DirtyOverflow)
    -- ^ Newly allocated overflow pages in this transaction.

    , writerOverflowCounter :: !Word32
    -- ^ Counts how many overflow pages were already allocated in this transaction.

    , writerRemovedOverflows :: ![OldOverflow]
    -- ^ Old overflow pages that were removed in this transaction
    -- and should be deleted when no longer in use.
    }

-- | Create a new writer.
newWriter :: hnd -> TxId -> Map TxId Integer
          -> S 'TypeData PageId     -> S 'TypeIndex PageId
          -> S 'TypeData [FreePage] -> S 'TypeIndex [FreePage]
          -> S 'TypeData FreeTree   -> S 'TypeIndex FreeTree
          -> WriterEnv hnd
newWriter hnd tx readers
          numDataPages numIndexPages
          dataFreePages indexFreePages
          dataFreeTree indexFreeTree =
   WriterEnv {
     writerHnds = hnd
   , writerTxId = tx
   , writerReaders = readers

   , writerIndexFileState = newFileState numIndexPages indexFreePages indexFreeTree
   , writerDataFileState = newFileState numDataPages dataFreePages dataFreeTree

   , writerQueryFreeTreeOn = True
   , writerDirtyOverflows = S.empty
   , writerOverflowCounter = 0
   , writerRemovedOverflows = []
   }
  where
    newFileState numPages freePages freeTree = FileState {
        fileStateNewlyFreedPages = []
      , fileStateOriginalNumPages = numPages
      , fileStateNewNumPages = numPages
      , fileStateDirtyPages = S.empty
      , fileStateCachedFreePages = freePages
      , fileStateFreeTree = freeTree
      }

-- | Wrapper around 'PageId' indicating it is newly free'd and cannot be reused
-- in the same transaction.
newtype NewlyFreed = NewlyFreed PageId deriving (Eq, Ord, Show)

-- | Wrapper around 'PageId' indicating it is free and can be reused in any
-- transaction.
newtype FreePage = FreePage PageId deriving (Binary, Eq, Ord, Show)

-- | Wrapper around 'PageId' indicating that it is dirty, i.e. written to in
-- this transaction.
newtype Dirty = Dirty PageId deriving (Eq, Ord, Show)

-- | Try to free a page, given a set of dirty pages.
--
-- If the page was dirty, a 'FreePage' page is added to the environment, if
-- not a 'NewlyFreed' page is added to the environment.
--
-- Btw, give me lenses...
freePage :: (Functor m, MonadState (WriterEnv hnd) m) => S stateType PageId -> m ()
freePage pid@(DataState pid') = do
    dirty'        <- dirty pid
    modify' $ \e ->
        e { writerDataFileState =
                updateFileState (writerDataFileState e) DataState
                                dirty' pid'
          }

freePage pid@(IndexState pid') = do
    dirty'        <- dirty pid
    modify' $ \e ->
        e { writerIndexFileState =
                updateFileState (writerIndexFileState e) IndexState
                                dirty' pid'
          }

updateFileState :: FileState t
                -> (forall a. a -> S t a)
                -> Maybe Dirty
                -> PageId
                -> FileState t
updateFileState e cons dirty' pid' =
  if | Just (Dirty p) <- dirty' ->
          e { fileStateCachedFreePages =
                cons $ FreePage p : getSValue (fileStateCachedFreePages e) }

     | p <- pid' ->
          e { fileStateNewlyFreedPages =
                NewlyFreed p : fileStateNewlyFreedPages e  }

-- | Get a 'Dirty' page, by first proving it is in fact dirty.
dirty :: (Functor m, MonadState (WriterEnv hnd) m) => S stateType PageId -> m (Maybe Dirty)
dirty pid = case pid of
    DataState p  -> (page p . fileStateDirtyPages . writerDataFileState) <$> get
    IndexState p -> (page p . fileStateDirtyPages . writerIndexFileState) <$> get
  where
    page p dirtyPages
        | p `S.member` dirtyPages = Just (Dirty p)
        | otherwise                         = Nothing

-- | Touch a fresh page, make it dirty.
--
-- We really need lenses...
touchPage :: MonadState (WriterEnv hnd) m => S stateType PageId -> m ()
touchPage (DataState pid) = do
    modify' $ \e ->
        let dirtyPages = fileStateDirtyPages (writerDataFileState e) in
        e { writerDataFileState = (writerDataFileState e) {
            fileStateDirtyPages = S.insert pid dirtyPages }
          }
    modify' $ \e ->
        let oldNum = getSValue $ fileStateNewNumPages (writerDataFileState e)
            newNum = max oldNum (pid + 1)
        in e { writerDataFileState = (writerDataFileState e) {
                fileStateNewNumPages = DataState newNum }
             }

touchPage (IndexState pid) = do
    modify' $ \e ->
        let dirtyPages = fileStateDirtyPages (writerIndexFileState e) in
        e { writerIndexFileState = (writerIndexFileState e) {
            fileStateDirtyPages = S.insert pid dirtyPages }
          }
    modify' $ \e ->
        let oldNum = getSValue $ fileStateNewNumPages (writerIndexFileState e)
            newNum = max oldNum (pid + 1)
        in e { writerIndexFileState = (writerIndexFileState e) {
                fileStateNewNumPages = IndexState newNum }
             }

-- | Wrapper around 'OverflowId' indicating that it is dirty.
newtype DirtyOverflow = DirtyOverflow OverflowId deriving (Eq, Ord, Show)

-- | Wrapper around 'OverflowId' indicating that it is an overflow
-- page from a previous transaction.
newtype OldOverflow = OldOverflow OverflowId deriving (Eq, Ord, Show)

-- | Touch a fresh overflow page, making it dirty.
touchOverflow :: MonadState (WriterEnv hnd) m => OverflowId -> m ()
touchOverflow i = modify' $
    \e -> e { writerDirtyOverflows =
        S.insert (DirtyOverflow i) (writerDirtyOverflows e) }

-- | Get the type of the overflow page.
overflowType ::  MonadState (WriterEnv hnd) m => OverflowId -> m (Either DirtyOverflow OldOverflow)
overflowType i = do
    dirty' <- gets $ \e -> S.member (DirtyOverflow i) (writerDirtyOverflows e)
    if dirty' then return $ Left  (DirtyOverflow i)
              else return $ Right (OldOverflow i)

-- | Free an old overflow page.
removeOldOverflow :: MonadState (WriterEnv hdn) m => OldOverflow -> m ()
removeOldOverflow i =
    modify' $ \e -> e { writerRemovedOverflows = i : writerRemovedOverflows e }
