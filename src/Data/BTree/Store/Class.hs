{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances #-}

module Data.BTree.Store.Class where

import Data.BTree.Primitives

import Control.Applicative (Applicative)
import Control.Monad.Trans
import Control.Monad.Trans.Reader (ReaderT)
import Control.Monad.Trans.State (StateT)

--------------------------------------------------------------------------------

class (Applicative m, Monad m) => StoreM hnd m | m -> hnd where
    setSize     ::  hnd -> PageCount -> m ()
    getSize     ::  hnd -> m PageCount
    getNodePage ::  (Key key, Value val)
                =>  hnd
                ->  Height height
                ->  NodeId height key val
                ->  m (Node height key val)
    putNodePage ::  (Key key, Value val)
                =>  hnd
                ->  Height height
                ->  NodeId height key val
                ->  Node height key val
                ->  m ()

instance StoreM hnd m => StoreM hnd (StateT s m) where
    setSize     = (lift.)       . setSize
    getSize     = lift          . getSize
    getNodePage = ((lift.).)    . getNodePage
    putNodePage = (((lift.).).) . putNodePage

instance StoreM hnd m => StoreM hnd (ReaderT s m) where
    setSize     = (lift.)       . setSize
    getSize     = lift          . getSize
    getNodePage = ((lift.).)    . getNodePage
    putNodePage = (((lift.).).) . putNodePage

--------------------------------------------------------------------------------
