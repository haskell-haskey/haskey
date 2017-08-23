module Database.Haskey.Alloc.Concurrent.FreePages.Save where

import Data.List.NonEmpty (NonEmpty((:|)))

import Data.BTree.Alloc.Class
import Data.BTree.Primitives

import Database.Haskey.Alloc.Concurrent.Environment
import Database.Haskey.Alloc.Concurrent.FreePages.Tree

-- | Save the free pages from the dirty page list and the free page
-- cache.
saveFreePages :: AllocM m
              => TxId
              -> FileState t
              -> m FreeTree
saveFreePages tx env = saveNewlyFreedPages tx env tree
                   >>= saveCachedFreePages env
  where
    tree = getSValue $ fileStateFreeTree env

-- | Save the newly free pages of the current transaction, as stored by
-- 'writerNewlyFreedPages'.
saveNewlyFreedPages :: AllocM m
                    => TxId
                    -> FileState t
                    -> FreeTree
                    -> m FreeTree
saveNewlyFreedPages tx env tree =
    case newlyFreed of
        [] -> deleteSubtree tx tree
        x:xs -> replaceSubtree tx (x :| xs) tree
  where
    newlyFreed = map (\(NewlyFreed pid) -> pid) $ fileStateNewlyFreedPages env

-- | Save the free apges from the free page cache in
-- 'writerReusablePages' using 'writerReuseablePagesTxId'.
saveCachedFreePages :: AllocM m
                    => FileState t
                    -> FreeTree
                    -> m FreeTree
saveCachedFreePages env tree = case fileStateReusablePagesTxId env of
    Nothing -> return tree
    Just k ->
        case freePages of
            [] -> deleteSubtree k tree
            x:xs -> replaceSubtree k (x :| xs) tree
  where
    freePages = map (\(OldFree pid) -> pid) $ fileStateReusablePages env
