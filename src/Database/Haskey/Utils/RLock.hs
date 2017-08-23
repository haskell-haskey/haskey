{-# LANGUAGE DeriveDataTypeable #-}
-- | Simple implementations of reentrant locks using 'MVar'
module Database.Haskey.Utils.RLock where

import Control.Concurrent (ThreadId, myThreadId)
import Control.Concurrent.MVar
import Control.Exception (Exception, throwIO)
import Control.Monad (unless, when)
import Control.Monad.Catch (MonadMask, bracket_)
import Control.Monad.IO.Class (MonadIO, liftIO)

import Data.Typeable (Typeable)

-- | A reentrant lock.
type RLock = (MVar (Maybe (ThreadId, Integer)), MVar ())

-- | Create a new reentrant lock.
newRLock :: IO RLock
newRLock = do { a <- newMVar Nothing; b <- newMVar (); return (a, b) }

-- | Acquire a reentrant lock, blocks.
acquireRLock :: RLock -> IO ()
acquireRLock (r, l) = do
    myId <- myThreadId
    ok <- modifyMVar r $ \state -> case state of
        Nothing -> return (state, False)
        Just (tId, x) -> if tId == myId
            then return (Just (myId, x + 1), True)
            else return (state, False)

    unless ok $ do
        () <- takeMVar l
        modifyMVar_ r $ const (return $ Just (myId, 1))

-- | Release a reentrant lock.
releaseRLock :: RLock -> IO ()
releaseRLock (r, l) = do
    myId <- myThreadId
    done <- modifyMVar r $ \state -> case state of
        Nothing -> throwIO $ RLockError "the lock has no inhabitant"
        Just (_, 0) -> throwIO $ RLockError "the lock is already released"
        Just (tId, n) -> if tId == myId
            then if n == 1
                    then return (Nothing, True)
                    else return (Just (myId, n-1), False)
            else throwIO $ RLockError "lock not held by releaser"

    when done $
        putMVar l ()

-- | Execute an action with the lock, bracketed, exception-safe
withRLock :: (MonadIO m, MonadMask m) => RLock -> m a -> m a
withRLock l = bracket_ (liftIO $ acquireRLock l)
                       (liftIO $ releaseRLock l)

-- | Exception raised when 'RLock' is improperly used.
newtype RLockError = RLockError String deriving (Show, Typeable)

instance Exception RLockError where
