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
  where
    tree = getSValue $ fileStateFreeTree env

-- | Save the newly free pages of the current transaction, as stored by
-- 'fileStateNewlyFreedPages'.
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
