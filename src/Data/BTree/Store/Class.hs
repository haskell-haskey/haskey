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
    {-| A function that calculates the hypothetical size of a node, if it were
       to be written to a page (regardless of the maximum page size). -}
    nodePageSize :: (Key key, Value val)
                 => m (Height height -> Node height key val -> PageSize)

    {-| The maximum page size the allocator can handle. -}
    maxPageSize  :: m PageSize

    {-| Directly set the amount of available physical pages. -}
    setSize      :: hnd -> PageCount -> m ()

    {-| Get the amount of physical available pages. -}
    getSize      :: hnd -> m PageCount

    {-| Read a page and return the actual node. -}
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

instance StoreM hnd m => StoreM hnd (StateT s m) where
    nodePageSize = lift              nodePageSize
    maxPageSize  = lift              maxPageSize
    setSize      = (lift.).          setSize
    getSize      = lift.             getSize
    getNodePage  = ((((lift.).).).). getNodePage
    putNodePage  = (((lift.).).).    putNodePage

instance StoreM hnd m => StoreM hnd (ReaderT s m) where
    nodePageSize = lift              nodePageSize
    maxPageSize  = lift              maxPageSize
    setSize      = (lift.).          setSize
    getSize      = lift.             getSize
    getNodePage  = ((((lift.).).).). getNodePage
    putNodePage  = (((lift.).).).    putNodePage

--------------------------------------------------------------------------------
