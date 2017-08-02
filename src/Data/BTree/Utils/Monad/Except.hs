module Data.BTree.Utils.Monad.Except where

import Control.Monad.Except

justErrM :: MonadError e m => e -> Maybe a -> m a
justErrM _ (Just v) = return v
justErrM e Nothing  = throwError e
