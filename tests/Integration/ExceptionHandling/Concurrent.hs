module Integration.ExceptionHandling.Concurrent where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.HUnit (testCase)
import Test.HUnit hiding (Test)

import Control.Monad.Catch
import Control.Monad.State

import Data.Maybe (isJust)

import Data.BTree.Store.Binary
import Data.BTree.Alloc.Concurrent.Monad

tests :: Test
tests = testGroup "ExceptionHandling.Concurrent"
    [ testCase "catch sync exceptions" case_catch_sync_exceptions
    ]

data TestEnv env = EnvInit
                 | EnvFinal
                 deriving (Eq, Show)

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

handle' :: Monad m
        => ConcurrentError (TestEnv ConcurrentHandles)
        -> m (Maybe (ConcurrentError (TestEnv ConcurrentHandles)))
handle' = return . Just

checkError :: Maybe (ConcurrentError (TestEnv ConcurrentHandles)) -> IO ()
checkError Nothing =
    assertBool "expected an exception, but none caught" False

checkError (Just (ConcurrentError e env)) = do
    assertEqual "" EnvFinal env
    assertBool ("expected TestError, but got " ++ show e) $
               isJust (fromException e :: Maybe TestError)
