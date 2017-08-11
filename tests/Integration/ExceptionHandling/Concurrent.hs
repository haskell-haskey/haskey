{-# LANGUAGE FlexibleContexts #-}
module Integration.ExceptionHandling.Concurrent where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.HUnit (testCase)
import Test.HUnit hiding (Test)

import Control.Concurrent
import Control.Exception (throwTo)
import Control.Monad (forever)
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.State

import Data.Maybe (isJust)

import Data.BTree.Store.Binary
import Data.BTree.Alloc.Concurrent.Environment
import Data.BTree.Alloc.Concurrent.Monad

import Debug.Trace

tests :: Test
tests = testGroup "ExceptionHandling.Concurrent"
    [ testCase "catch sync exceptions"  case_catch_sync_exceptions
    --, testCase "catch async exceptions" case_catch_async_exceptions
    ]

data TestEnv env = EnvInit
                 | EnvFinal
                 deriving (Eq, Show)

instance RecoverableState (TestEnv env) where
    recover EnvInit  = Nothing
    recover EnvFinal = finalState

data TestError = TestError deriving (Show)

instance Exception TestError where

case_catch_sync_exceptions :: Assertion
case_catch_sync_exceptions = do
    v <- handle handle' $ do
        _ <- evalStoreT (runConcurrentT action EnvInit) emptyStore
        return Nothing
    checkError v
  where
    action = put EnvFinal >> throwM TestError

{-
case_catch_async_exceptions :: Assertion
case_catch_async_exceptions = do
    c   <- newEmptyMVar
    v   <- newEmptyMVar
    pid <- forkIO $ do
        v' <- handle handle' $ do
            _ <- evalStoreT (trace "running" $ runConcurrentT (action c) EnvInit) emptyStore
            return Nothing
        putMVar v v'

    () <- takeMVar c
    trace "throwing" $ throwTo pid TestError
    v' <- takeMVar v
    checkError v'
  where
    action c = do
        put EnvFinal
        liftIO $ putMVar c ()
        forever $ liftIO (threadDelay 1000000)
-}

handle' :: Monad m
        => ConcurrentError
        -> m (Maybe ConcurrentError)
handle' = return . Just

checkError :: Maybe ConcurrentError -> IO ()
checkError Nothing =
    assertBool "expected an exception, but none caught" False

checkError (Just (ConcurrentError e s)) = do
    assertEqual "" finalState s
    assertBool ("expected TestError, but got " ++ show e) $
               isJust (fromException e :: Maybe TestError)

finalState :: Maybe RecoveredState
finalState = Just $ RecoveredState 1
