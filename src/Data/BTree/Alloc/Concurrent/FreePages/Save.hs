module Data.BTree.Alloc.Concurrent.FreePages.Save where

import Data.List.NonEmpty (NonEmpty((:|)))

import Data.BTree.Alloc.Class
import Data.BTree.Alloc.Concurrent.Environment
import Data.BTree.Alloc.Concurrent.FreePages.Tree

-- | Save the free pages from the dirty page list and the free page
-- cache.
saveFreePages :: AllocM m
              => WriterEnv hnd
              -> m FreeTree
saveFreePages env = saveNewlyFreedPages env tree
                >>= saveCachedFreePages env
  where
    tree = writerFreeTree env

-- | Save the newly free pages of the current transaction, as stored by
-- 'writerNewlyFreedPages'.
saveNewlyFreedPages :: AllocM m
                            => WriterEnv hnd
                            -> FreeTree
                            -> m FreeTree
saveNewlyFreedPages env tree =
    case newlyFreed of
        [] -> deleteSubtree (writerTxId env) tree
        x:xs -> replaceSubtree (writerTxId env) (x :| xs) tree
  where
    newlyFreed = map (\(NewlyFreed pid) -> pid) $ writerNewlyFreedPages env

-- | Save the free apges from the free page cache in
-- 'writerReusablePages' using 'writerReuseablePagesTxId'.
saveCachedFreePages :: AllocM m
                    => WriterEnv hnd
                    -> FreeTree
                    -> m FreeTree
saveCachedFreePages env tree = case writerReusablePagesTxId env of
    Nothing -> return tree
    Just k ->
        case freePages of
            [] -> deleteSubtree k tree
            x:xs -> replaceSubtree k (x :| xs) tree
  where
    freePages = map (\(OldFree pid) -> pid) $ writerReusablePages env
