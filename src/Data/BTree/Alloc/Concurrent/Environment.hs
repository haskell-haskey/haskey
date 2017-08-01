-- | Environments of a read or write transaction.
module Data.BTree.Alloc.Concurrent.Environment where

import Data.Set (Set)

import STMContainers.Map (Map)

import Data.BTree.Impure
import Data.BTree.Primitives

newtype ReaderEnv hnd = ReaderEnv { readerHnd :: hnd }

data WriterEnv hnd = WriterEnv
    { writerHnd :: !hnd
    , writerTxId :: !TxId
    , writerReaders :: Map TxId Integer
    , writerNewlyFreedPages :: ![PageId] -- ^ Pages free'd in this transaction,
                                         -- not ready for reuse until the
                                         -- transaction is commited.
    , writerAllocdPages :: !(Set PageId) -- ^ Pages allocated in this transcation.
                                         -- These pages can be reused in the same
                                         -- transaction if free'd later.
    , writerFreedDirtyPages :: ![PageId] -- ^ Pages free for immediate reuse.
    , writerFreeTree :: !(Tree TxId [PageId]) -- ^ The root of the free tree,
                                            -- might change during a
                                            -- transaction.
    , writerReuseablePages :: ![PageId] -- ^ Pages queried from the free pages
                                        -- database and ready for immediate reuse.
    , writerReuseablePagesTxId :: !(Maybe TxId) -- ^ The 'TxId' of the pages in
                                                -- 'writerReuseablePages', or 'Nothing'
                                                -- if no pages were queried yet from
                                                -- the free database.
    , writerReusablePagesOn :: !Bool -- ^ Used to turn of querying the free page
                                     -- database for free pages.
    }
