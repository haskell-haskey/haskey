module Data.BTree.Utils.Monad where

ifM :: Monad m => m Bool -> m a -> m a -> m a
ifM f y n = do f' <- f; if f' then y else n
