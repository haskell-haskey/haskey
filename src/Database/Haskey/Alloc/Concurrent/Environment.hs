{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE StandaloneDeriving #-}
-- | Environments of a read or write transaction.
module Database.Haskey.Alloc.Concurrent.Environment where

import Control.Applicative ((<$>))
import Control.Monad.State

import Data.Binary (Binary)
import Data.Set (Set)
import Data.Word (Word32)
import qualified Data.Binary as B
import qualified Data.Set as S

import STMContainers.Map (Map)

import Data.BTree.Primitives

import Database.Haskey.Alloc.Concurrent.FreePages.Tree

data StateType = TypeData
               | TypeIndex

-- | Wrapper around a type to indicate it belongs to a file with either
-- data/leaf nodes or index nodes.
data S (t :: StateType) a where
    DataState  :: a -> S 'TypeData  a
    IndexState :: a -> S 'TypeIndex a

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

    , fileStateFreedDirtyPages :: !(S stateType (Set DirtyFree))
    -- ^ Pages freshly allocated AND free'd in this transaction. Immediately
    -- ready for reuse.

    , fileStateFreeTree :: !(S stateType FreeTree)
    -- ^ The root of the free tree, might change during a transaction.

    , fileStateDirtyReusablePages :: !(Set DirtyOldFree)
    -- ^ All pages queried from the free page database for
    -- 'fileStateReusablePagesTxId', and actually used once already.

    , fileStateReusablePages :: ![OldFree]
    -- ^ Pages queried from the free pages database and ready for immediate
    -- reuse.

    , fileStateReusablePagesTxId :: !(Maybe TxId)
    -- ^ The 'TxId' of the pages in 'fileStateReusablePages', or 'Nothing' if no
    -- pages were queried yet from the free database.

    }

data WriterEnv hnds = WriterEnv
    { writerHnds :: !hnds
    , writerTxId :: !TxId
    , writerReaders :: Map TxId Integer

    , writerIndexFileState :: FileState 'TypeIndex
    -- ^ State of the file with index nodes.

    , writerDataFileState :: FileState 'TypeData
    -- ^ State of the file with data/leaf nodes.

    , writerReusablePagesOn :: !Bool
    -- ^ Used to turn of querying the free page database for free pages.

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
          -> S 'TypeData PageId          -> S 'TypeIndex PageId
          -> S 'TypeData (Set DirtyFree) -> S 'TypeIndex (Set DirtyFree)
          -> S 'TypeData FreeTree        -> S 'TypeIndex FreeTree
          -> WriterEnv hnd
newWriter hnd tx readers
          numDataPages numIndexPages
          dataDirtyFree indexDirtyFree
          dataFreeTree indexFreeTree =
   WriterEnv {
     writerHnds = hnd
   , writerTxId = tx
   , writerReaders = readers

   , writerIndexFileState = newFileState numIndexPages indexDirtyFree indexFreeTree
   , writerDataFileState = newFileState numDataPages dataDirtyFree dataFreeTree

   , writerReusablePagesOn = True
   , writerDirtyOverflows = S.empty
   , writerOverflowCounter = 0
   , writerRemovedOverflows = []
   }
  where
    newFileState numPages dirtyFree freeTree = FileState {
        fileStateNewlyFreedPages = []
      , fileStateOriginalNumPages = numPages
      , fileStateNewNumPages = numPages
      , fileStateFreedDirtyPages = dirtyFree
      , fileStateFreeTree = freeTree
      , fileStateDirtyReusablePages = S.empty
      , fileStateReusablePages = []
      , fileStateReusablePagesTxId = Nothing
      }

-- | Wrapper around 'PageId' indicating it is a fresh page, allocated at the
-- end of the database.
newtype Fresh = Fresh PageId deriving (Eq, Ord, Show)

-- | Wrapper around 'PageId' indicating it is newly free'd and cannot be reused
-- in the same transaction.
newtype NewlyFreed = NewlyFreed PageId deriving (Eq, Ord, Show)

-- | Wrapper around 'PageId' indicating it is a dirty page.
newtype Dirty = Dirty PageId deriving (Eq, Ord, Show)

-- | Wrapper around 'PageId' indicating the page is dirty and free for reuse.
newtype DirtyFree = DirtyFree PageId deriving (Binary, Eq, Ord, Show)

-- | Wrapper around 'PageId' inidcating it was fetched from the free database
-- and is ready for reuse.
newtype OldFree = OldFree PageId deriving (Eq, Ord, Show)

-- | Wrapper around 'PageId' indicating it wa fetched from the free database
-- and is actually dirty.
newtype DirtyOldFree = DirtyOldFree PageId deriving (Eq, Ord, Show)

-- | A sum type repesenting any type of free page, that can immediately be used
-- to write something to.
data SomeFreePage = FreshFreePage Fresh
                  | DirtyFreePage DirtyFree
                  | OldFreePage OldFree

getSomeFreePageId :: SomeFreePage -> PageId
getSomeFreePageId (FreshFreePage (Fresh     pid)) = pid
getSomeFreePageId (DirtyFreePage (DirtyFree pid)) = pid
getSomeFreePageId (OldFreePage   (OldFree   pid)) = pid

-- | Try to free a page, given a set of dirty pages.
--
-- If the page was dirty, a 'DirtyFree' page is added to the environment, if
-- not a 'NewlyFreed' page is added to the environment.
--
-- Btw, give me lenses...
freePage :: (Functor m, MonadState (WriterEnv hnd) m) => S stateType PageId -> m ()
freePage pid@(DataState pid') = do
    dirty'        <- dirty pid
    dirtyOldFree' <- dirtyOldFree pid
    modify' $ \e ->
        e { writerDataFileState =
                updateFileState (writerDataFileState e) DataState
                                dirty' dirtyOldFree' pid'
          }

freePage pid@(IndexState pid') = do
    dirty'        <- dirty pid
    dirtyOldFree' <- dirtyOldFree pid
    modify' $ \e ->
        e { writerIndexFileState =
                updateFileState (writerIndexFileState e) IndexState
                                dirty' dirtyOldFree' pid'
          }

updateFileState :: FileState t
                -> (forall a. a -> S t a)
                -> Maybe Dirty
                -> Maybe DirtyOldFree
                -> PageId
                -> FileState t
updateFileState e cons dirty' dirtyOldFree' pid' =
  if | Just (Dirty p) <- dirty' ->
          e { fileStateFreedDirtyPages =
                cons $ S.insert (DirtyFree p) (getSValue $ fileStateFreedDirtyPages e) }

     | Just (DirtyOldFree p) <- dirtyOldFree' ->
          e { fileStateReusablePages =
                OldFree p : fileStateReusablePages e }

     | p <- pid' ->
          e { fileStateNewlyFreedPages =
                NewlyFreed p : fileStateNewlyFreedPages e  }

-- | Get a 'Dirty' page, by first proving it is in fact dirty.
dirty :: (Functor m, MonadState (WriterEnv hnd) m) => S stateType PageId -> m (Maybe Dirty)
dirty pid = case pid of
    DataState p  -> (page p . fileStateOriginalNumPages . writerDataFileState) <$> get
    IndexState p -> (page p . fileStateOriginalNumPages . writerIndexFileState) <$> get
  where
    page p origNumPages
        | p >= getSValue origNumPages = Just (Dirty p)
        | otherwise                   = Nothing

-- | Get a 'DirtyOldFree' page, by first proving it is in fact a dirty old free page.
dirtyOldFree :: (Functor m, MonadState (WriterEnv hnd) m) => S stateType PageId -> m (Maybe DirtyOldFree)
dirtyOldFree pid = case pid of
    DataState p  -> (page p . fileStateDirtyReusablePages . writerDataFileState) <$> get
    IndexState p -> (page p . fileStateDirtyReusablePages . writerIndexFileState) <$> get
  where
    page p dirty'
        | S.member (DirtyOldFree p) dirty' = Just (DirtyOldFree p)
        | otherwise                        = Nothing


-- | Touch a fresh page, make it dirty.
--
-- We really need lenses...
touchPage :: MonadState (WriterEnv hnd) m => S stateType SomeFreePage -> m ()
touchPage (DataState (DirtyFreePage _)) = return()
touchPage (IndexState (DirtyFreePage _)) = return ()

touchPage (DataState (FreshFreePage (Fresh pid))) = modify' $ \e ->
    case fileStateNewNumPages (writerDataFileState e) of
        DataState numPages ->
            if numPages < pid + 1
                then e { writerDataFileState = (writerDataFileState e) {
                            fileStateNewNumPages = DataState (pid + 1) }
                       }
                else e
touchPage (IndexState (FreshFreePage (Fresh pid))) = modify' $ \e ->
    case fileStateNewNumPages (writerIndexFileState e) of
        IndexState numPages ->
            if numPages < pid + 1
                then e { writerIndexFileState = (writerIndexFileState e) {
                            fileStateNewNumPages = IndexState (pid + 1) }
                       }
                else e

touchPage (DataState (OldFreePage (OldFree pid))) = modify' $ \e ->
    let s = fileStateDirtyReusablePages (writerDataFileState e) in
    e { writerDataFileState = (writerDataFileState e) {
            fileStateDirtyReusablePages = S.insert (DirtyOldFree pid) s }
      }
touchPage (IndexState (OldFreePage (OldFree pid))) = modify' $ \e ->
    let s = fileStateDirtyReusablePages (writerIndexFileState e) in
    e { writerIndexFileState = (writerIndexFileState e) {
            fileStateDirtyReusablePages = S.insert (DirtyOldFree pid) s }
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
