module Data.BTree.Alloc.Concurrent.FreePages.Save where

import Control.Monad ((>=>))

import Data.BTree.Alloc.Class
import Data.BTree.Alloc.Concurrent.Environment
import Data.BTree.Alloc.Concurrent.FreePages.Tree

-- | Save the free pages from the dirty page list and the free page
-- cache.
saveFreePages :: AllocM m
              => WriterEnv hnd
              -> FreeTree
              -> m FreeTree
saveFreePages env = saveNewlyAndDirtyFreedPages env
                >=> saveCachedFreePages env

-- | Save the newly free pages and the freed dirty pages of the current
-- transaction, as stored by 'writerNewlyFreedPages' and
-- 'writerFreedDirtyPages'.
saveNewlyAndDirtyFreedPages :: AllocM m
                            => WriterEnv hnd
                            -> FreeTree
                            -> m FreeTree
saveNewlyAndDirtyFreedPages env tree =
    case writerNewlyFreedPages env {-++ writerFreedDirtyPages env-} of
        [] -> return tree
        xs -> insertSubtree (writerTxId env) xs tree

-- | Save the free apges from the free page cache in
-- 'writerReuseablePages' using 'writerReuseablePagesTxId'.
saveCachedFreePages :: AllocM m
                    => WriterEnv hnd
                    -> FreeTree
                    -> m FreeTree
saveCachedFreePages env tree = case writerReuseablePagesTxId env of
    Nothing -> return tree
    Just k ->
        case writerReuseablePages env of
            [] -> deleteSubtree k tree
            xs -> replaceSubtree k xs tree
