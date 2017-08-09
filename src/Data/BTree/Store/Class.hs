{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances #-}
{-| A storage back-end manages physical storage of pages. -}
module Data.BTree.Store.Class where

import Data.BTree.Impure
import Data.BTree.Primitives

import Control.Applicative (Applicative)
import Control.Monad.Trans
import Control.Monad.Trans.Reader (ReaderT)
import Control.Monad.Trans.State (StateT)

import Data.Proxy

--------------------------------------------------------------------------------

{-| A storage back-end that can store and fetch physical pages. -}
class (Applicative m, Monad m) => StoreM hnd m | m -> hnd where
    {-| Open a database handle for reading and writing. -}
    openHandle :: hnd -> m ()

    {-| Close a database handle. -}
    closeHandle :: hnd -> m ()

    {-| Remove a handle from the storage back-end. -}
    removeHandle :: hnd -> m ()

    {-| A function that calculates the hypothetical size of a node, if it were
       to be written to a page (regardless of the maximum page size). -}
    nodePageSize :: (Key key, Value val)
                 => m (Height height -> Node height key val -> PageSize)

    {-| The maximum page size the allocator can handle. -}
    maxPageSize  :: m PageSize

    {-| Get a new unused fresh 'PageId' from the end of the file. -}
    newPageId    :: hnd -> m PageId

    {-| Read a page and return the actual node and the transaction id when the
       node was written. -}
    getNodePage  :: (Key key, Value val)
                 => hnd
                 -> Height height
                 -> Proxy key
                 -> Proxy val
                 -> NodeId height key val
                 -> m (Node height key val)

    {-| Write a node to a physical page. -}
    putNodePage  :: (Key key, Value val)
                 => hnd
                 -> Height height
                 -> NodeId height key val
                 -> Node height key val
                 -> m ()

    {-| Read a value from an overflow page -}
    getOverflow :: (Value val)
                => hnd
                -> Proxy val
                -> m val

    {-| Write a value to an overflow page -}
    putOverflow :: (Value val)
                => hnd
                -> val
                -> m ()


instance StoreM hnd m => StoreM hnd (StateT s m) where
    openHandle   = lift.             openHandle
    closeHandle  = lift.             closeHandle
    removeHandle = lift.             closeHandle
    nodePageSize = lift              nodePageSize
    maxPageSize  = lift              maxPageSize
    newPageId    = lift.             newPageId
    getNodePage  = ((((lift.).).).). getNodePage
    putNodePage  = (((lift.).).).    putNodePage
    getOverflow  = (lift.).          getOverflow
    putOverflow  = (lift.).          putOverflow

instance StoreM hnd m => StoreM hnd (ReaderT s m) where
    openHandle   = lift.             openHandle
    closeHandle  = lift.             closeHandle
    removeHandle = lift.             closeHandle
    nodePageSize = lift              nodePageSize
    maxPageSize  = lift              maxPageSize
    newPageId    = lift.             newPageId
    getNodePage  = ((((lift.).).).). getNodePage
    putNodePage  = (((lift.).).).    putNodePage
    getOverflow  = (lift.).          getOverflow
    putOverflow  = (lift.).          putOverflow

--------------------------------------------------------------------------------
