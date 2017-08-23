module Database.Haskey.Utils.Monad.Catch where

import Control.Monad.Catch

justErrM :: (MonadThrow m, Exception e) => e -> Maybe a -> m a
justErrM _ (Just v) = return v
justErrM e Nothing  = throwM e
