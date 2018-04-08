module Main where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async, wait)
import Control.Monad (void, replicateM)
import Control.Monad.Catch (bracket_, finally)

import Data.BTree.Impure (Tree)
import Data.ByteString (ByteString)
import Data.Int (Int32)
import Data.Text.Encoding (encodeUtf8)
import qualified Data.BTree.Impure as B
import qualified Data.Text as Text

import Database.Haskey.Alloc.Concurrent (ConcurrentDb,
                                         ConcurrentHandles,
                                         concurrentHandles,
                                         lockConcurrentDb,
                                         unlockConcurrentDb,
                                         openConcurrentDb,
                                         createConcurrentDb,
                                         transact_,
                                         transactReadOnly,
                                         commit_)
import Database.Haskey.Store.File (FileStoreT, runFileStoreT, defFileStoreConfig)
import Database.Haskey.Store.InMemory (MemoryStoreT, MemoryFiles, newEmptyMemoryStore,
                                       runMemoryStoreT, defMemoryStoreConfig)

import System.Directory (removeDirectoryRecursive)
import System.Random (randomIO)

concurrency :: Integral a => a
concurrency = 100

type Root = Tree Int32 ByteString

main :: IO ()
main = do
    inMemoryMain root
    fileMain root `finally` delRoot
  where
    root = "example-database.haskey"
    delRoot = removeDirectoryRecursive root

inMemoryMain :: FilePath -> IO ()
inMemoryMain root = do
    store <- newEmptyMemoryStore
    db    <- openOrCreate store

    writers <- mapM (async . writer store db) [1..concurrency]
    readers <- replicateM concurrency . async $ do
        delay <- randomIO
        reader store db (delay `rem` 5000)
    mapM_ wait writers
    mapM_ wait readers
    putStrLn "InMemory: done"
  where
    writer :: MemoryFiles FilePath
           -> ConcurrentDb Root
           -> Int32
           -> IO ()
    writer store db i =
        runDatabase store $ transact_ tx db
      where
        bs = encodeUtf8 $ Text.pack (show i)

        tx tree = B.insert i bs tree >>= commit_

    reader :: MemoryFiles FilePath
           -> ConcurrentDb Root
           -> Int
           -> IO ()
    reader files db delay = void $ replicateM 10 $ do
        threadDelay delay
        runDatabase files $ transactReadOnly B.toList db

    openOrCreate :: MemoryFiles FilePath
                 -> IO (ConcurrentDb Root)
    openOrCreate store = runDatabase store $ do
        maybeDb <- openConcurrentDb handles
        case maybeDb of
            Nothing -> createConcurrentDb handles B.empty
            Just db -> return db

    runDatabase :: MemoryFiles FilePath
                -> MemoryStoreT FilePath m a
                -> m a
    runDatabase files action = runMemoryStoreT action defMemoryStoreConfig files

    handles :: ConcurrentHandles
    handles = concurrentHandles root

fileMain :: FilePath -> IO ()
fileMain root = bracket_ (runDatabase $ lockConcurrentDb handles)
                         (runDatabase $ unlockConcurrentDb handles) $ do

    db <- openOrCreate
    writers <- mapM (async . writer db) [1..concurrency]
    readers <- replicateM concurrency . async $ do
        delay <- randomIO
        reader db (delay `rem` 5000)
    mapM_ wait writers
    mapM_ wait readers
    putStrLn "File: done"
  where
    writer :: ConcurrentDb Root
           -> Int32
           -> IO ()
    writer db i =
        runDatabase $ transact_ tx db
      where
        bs = encodeUtf8 $ Text.pack (show i)

        tx tree = B.insert i bs tree >>= commit_

    reader :: ConcurrentDb Root
           -> Int
           -> IO ()
    reader db delay = void $ replicateM 10 $ do
        threadDelay delay
        runDatabase $ transactReadOnly B.toList db

    openOrCreate :: IO (ConcurrentDb Root)
    openOrCreate = runDatabase $ do
        maybeDb <- openConcurrentDb handles
        case maybeDb of
            Nothing -> createConcurrentDb handles B.empty
            Just db -> return db

    runDatabase :: Monad m
                => FileStoreT FilePath m a
                -> m a
    runDatabase action = runFileStoreT action defFileStoreConfig

    handles :: ConcurrentHandles
    handles = concurrentHandles root
