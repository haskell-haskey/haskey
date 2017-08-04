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
saveFreePages env = saveNewlyAndDirtyFreedPages env tree
                >>= saveCachedFreePages env
  where
    tree = writerFreeTree env

-- | Save the newly free pages and the freed dirty pages of the current
-- transaction, as stored by 'writerNewlyFreedPages' and
-- 'writerFreedDirtyPages'.
saveNewlyAndDirtyFreedPages :: AllocM m
                            => WriterEnv hnd
                            -> FreeTree
                            -> m FreeTree
saveNewlyAndDirtyFreedPages env tree =
    case newlyFreed {-++ writerFreedDirtyPages env-} of
        [] -> return tree
        x:xs -> insertSubtree (writerTxId env) (x :| xs) tree
  where
    newlyFreed = map (\(NewlyFreed pid) -> pid) $ writerNewlyFreedPages env
    --freedDirty = map (\(DirtyFree  pid) -> pid) $ writerDirtyPages env

-- | Save the free apges from the free page cache in
-- 'writerReuseablePages' using 'writerReuseablePagesTxId'.
saveCachedFreePages :: AllocM m
                    => WriterEnv hnd
                    -> FreeTree
                    -> m FreeTree
saveCachedFreePages env tree = case writerReuseablePagesTxId env of
    Nothing -> return tree
    Just k ->
        case freePages of
            [] -> deleteSubtree k tree
            x:xs -> replaceSubtree k (x :| xs) tree
  where
    freePages = map (\(Free pid) -> pid) $ writerReuseablePages env
