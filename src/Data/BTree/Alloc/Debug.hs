{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
-- | An in memory allocator for debugging and testing purposes.
module Data.BTree.Alloc.Debug where

import Control.Monad.Identity
import Control.Monad.State

import Data.Map (Map, (!))
import Data.Word (Word64)
import qualified Data.ByteString as BS
import qualified Data.Map as M

import Unsafe.Coerce (unsafeCoerce)

import Data.BTree.Alloc.Class
import Data.BTree.Impure
import Data.BTree.Primitives
import Data.BTree.Store.Page

data SomeNode = forall h k v. SomeNode (Height h) (Node h k v)

getSomeNode :: SomeNode -> Node h k v
getSomeNode (SomeNode _ n) = unsafeCoerce n

data SomeVal = forall v. SomeVal v

getSomeVal :: SomeVal -> v
getSomeVal (SomeVal v) = unsafeCoerce v

data Pages = Pages {
    pagesNodes :: Map PageId SomeNode
  , pagesOverflow :: Map Word64 SomeVal
  }

emptyPages :: Pages
emptyPages = Pages {
    pagesNodes = M.empty
  , pagesOverflow = M.empty
  }

newtype DebugT m a = DebugT { runDebugT :: StateT Pages m a }
                   deriving (Functor, Applicative, Monad, MonadIO, MonadState Pages)

runDebug :: Pages -> DebugT Identity a -> (a, Pages)
runDebug pages = runIdentity . flip runStateT pages . runDebugT

evalDebug :: Pages -> DebugT Identity a -> a
evalDebug pages = fst . runDebug pages

instance Monad m => AllocReaderM (DebugT m) where
    readNode _ nid = do
        n <- gets (\pgs -> pagesNodes pgs ! nodeIdToPageId nid)
        return $ getSomeNode n

    readOverflow _ c = do
        v <- gets (\pgs -> pagesOverflow pgs ! c)
        return $ getSomeVal v

instance Monad m => AllocM (DebugT m) where
    nodePageSize = return $ \h ->
        fromIntegral . BS.length . encode . NodePage h

    maxPageSize = return 256

    allocNode h n = do
        pid <- fromIntegral <$> gets (M.size . pagesNodes)
        let n' = SomeNode h n
        modify' $ \pgs -> pgs { pagesNodes = M.insert pid n' (pagesNodes pgs) }
        return $ pageIdToNodeId pid

    freeNode _ _ = return ()

    allocOverflow v = do
        let v' = SomeVal v
        c <- fromIntegral <$> gets (M.size . pagesOverflow)
        modify' $ \pgs -> pgs { pagesOverflow = M.insert c v' (pagesOverflow pgs) }
        return (0, c)

    freeOverflow _ c =
        modify' $ \pgs -> pgs { pagesOverflow = M.delete c (pagesOverflow pgs) }
