{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Integration.CreateAndOpen where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.HUnit (testCase)
import Test.HUnit hiding (Test, Node)

import Control.Applicative ((<$>))

import Data.Binary (Binary)
import Data.Maybe (fromJust)

import System.Directory (removeDirectoryRecursive,
                         getTemporaryDirectory, doesDirectoryExist,
                         writable, getPermissions)
import System.IO.Temp (createTempDirectory)

import Data.BTree.Primitives (Value)

import Database.Haskey.Alloc.Concurrent
import Database.Haskey.Store.File

tests :: Test
tests = testGroup "CreateAndOpen"
    [ testCase "file backend" case_file_backend
    ]

case_file_backend :: Assertion
case_file_backend = do
    exists <- doesDirectoryExist "/var/run/shm"
    w      <- if exists then writable <$> getPermissions "/var/run/shm"
                        else return False
    tmpDir <- if w then return "/var/run/shm"
                   else getTemporaryDirectory
    fp     <- createTempDirectory tmpDir "db.haskey"
    let hnds = concurrentHandles fp

    _     <- create hnds
    root' <- open hnds

    removeDirectoryRecursive fp

    assertEqual "should've read back initial root" (Just root) root'
  where
    create :: ConcurrentHandles -> IO (ConcurrentDb TestRoot)
    create hnds = runFileStoreT (createConcurrentDb hnds root) config

    open :: ConcurrentHandles -> IO (Maybe TestRoot)
    open hnds = do
        maybeDb <- runFileStoreT (openConcurrentDb hnds) config
        case maybeDb of
            Nothing -> return Nothing
            Just db -> Just <$> runFileStoreT (transactReadOnly return db) config

    config = fromJust $ fileStoreConfigWithPageSize 256

    root = TestRoot "Hello World!"

newtype TestRoot = TestRoot String deriving (Binary, Eq, Value, Show)

instance Root TestRoot where
