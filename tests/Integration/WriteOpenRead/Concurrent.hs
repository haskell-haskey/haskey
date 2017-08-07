{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
module Integration.WriteOpenRead.Concurrent where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck.Monadic

import Control.Applicative ((<$>))
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Maybe

import Data.Foldable (foldlM)
import Data.Map (Map)
import Data.Maybe (isJust)
import qualified Data.Map as M

import System.Directory (removeDirectory, removeFile,
                         getTemporaryDirectory, doesDirectoryExist,
                         writable, getPermissions)
import System.FilePath ((</>))
import System.IO.Temp (createTempDirectory)

import Data.BTree.Alloc.Class
import Data.BTree.Alloc.Concurrent
import Data.BTree.Impure
import Data.BTree.Primitives
import Data.BTree.Store.Binary
import qualified Data.BTree.Impure as Tree
import qualified Data.BTree.Store.File as FS

import Integration.WriteOpenRead.Transactions

tests :: Test
tests = testGroup "WriteOpenRead.Concurrent"
    [ testProperty "memory backend" (monadicIO prop_memory_backend)
    , testProperty "file backend" (monadicIO prop_file_backend)
    ]

prop_memory_backend :: PropertyM IO ()
prop_memory_backend = forAllM genTestSequence $ \(TestSequence txs) -> do
    (Right db, files) <- run create
    result <- run . runMaybeT $ foldlM (\(files', m) tx -> writeReadTest db files' tx m)
                                       (files, M.empty)
                                       txs
    assert $ isJust result
  where

    writeReadTest :: ConcurrentDb Integer Integer
                  -> Files String
                  -> TestTransaction Integer Integer
                  -> Map Integer Integer
                  -> MaybeT IO (Files String, Map Integer Integer)
    writeReadTest db files tx m = do
        files'   <- openAndWrite db files tx
        read'    <- openAndRead db files'
        let expected = testTransactionResult m tx
        if read' == M.toList expected
            then return (files', expected)
            else error $ "error:"
                    ++ "\n    after:   " ++ show tx
                    ++ "\n    expectd: " ++ show (M.toList expected)
                    ++ "\n    got:     " ++ show read'

    create :: IO (Either String (ConcurrentDb Integer Integer), Files String)
    create = flip runStoreT emptyStore $ do
        openConcurrentHandles hnds
        createConcurrentDb hnds
      where
        hnds = defaultConcurrentHandles

    openAndRead db files = evalStoreT (readAll db) files >>= \case
        Left err -> error err
        Right v -> return v

    openAndWrite db files tx = runStoreT (writeTransaction tx db) files
        >>= \case
            (Left err, _) -> error $ "while writing: " ++ show err
            (Right _, files') -> return files'

--------------------------------------------------------------------------------

prop_file_backend :: PropertyM IO ()
prop_file_backend = forAllM genTestSequence $ \(TestSequence txs) -> do
    exists <- run $ doesDirectoryExist "/var/run/shm"
    w      <- if exists then run $ writable <$> getPermissions "/var/run/shm"
                        else return False
    tmpDir <- if w then return "/var/run/shm"
                   else run getTemporaryDirectory
    fp     <- run $ createTempDirectory tmpDir "db.haskey"
    let hnds = ConcurrentHandles {
        concurrentHandlesMain      = fp </> "main.db"
      , concurrentHandlesMetadata1 = fp </> "meta.md1"
      , concurrentHandlesMetadata2 = fp </> "meta.md2"
      }

    (Right db, files) <- run $ create hnds
    result <- run . runMaybeT $ foldM (writeReadTest db files)
                                      M.empty
                                      txs

    _ <- FS.runStoreT (closeConcurrentHandles hnds) files

    run $ removeFile (concurrentHandlesMain hnds)
    run $ removeFile (concurrentHandlesMetadata1 hnds)
    run $ removeFile (concurrentHandlesMetadata2 hnds)
    run $ removeDirectory fp

    assert $ isJust result
  where
    writeReadTest :: ConcurrentDb Integer Integer
                  -> FS.Files FilePath
                  -> Map Integer Integer
                  -> TestTransaction Integer Integer
                  -> MaybeT IO (Map Integer Integer)
    writeReadTest db files m tx = do
        _     <- lift $ openAndWrite db files tx
        read' <- lift $ openAndRead db files
        let expected = testTransactionResult m tx
        if read' == M.toList expected
            then return expected
            else error $ "error:"
                    ++ "\n    after:   " ++ show tx
                    ++ "\n    expectd: " ++ show (M.toList expected)
                    ++ "\n    got:     " ++ show read'

    create :: ConcurrentHandles
           -> IO (Either String (ConcurrentDb Integer Integer), FS.Files FilePath)
    create hnds = flip FS.runStoreT FS.emptyStore $ do
        openConcurrentHandles hnds
        createConcurrentDb hnds

    openAndRead :: ConcurrentDb Integer Integer
                -> FS.Files FilePath
                -> IO [(Integer, Integer)]
    openAndRead db files = FS.evalStoreT (readAll db) files
        >>= \case
            Left err -> error err
            Right v -> return v

    openAndWrite :: ConcurrentDb Integer Integer
                 -> FS.Files FilePath
                 -> TestTransaction Integer Integer
                 -> IO (FS.Files FilePath)
    openAndWrite db files tx = FS.runStoreT (void $ writeTransaction tx db) files
        >>= \case
            (Left err, _)    -> error $ "while writing: " ++ show err
            (Right _, files') -> return files'

--------------------------------------------------------------------------------

writeTransaction :: (MonadIO m, ConcurrentMetaStoreM m, Key k, Value v)
                 => TestTransaction k v
                 -> ConcurrentDb k v
                 -> m ()
writeTransaction (TestTransaction txType actions) =
    transaction
  where
    writeAction (Insert k v)  = insertTree k v
    writeAction (Replace k v) = insertTree k v
    writeAction (Delete k)    = deleteTree k

    transaction = transact_ $
        foldl (>=>) return (map writeAction actions)
        >=> commitOrAbort

    commitOrAbort :: AllocM n => Tree key val -> n (Transaction key val ())
    commitOrAbort
        | TxAbort  <- txType = const abort_
        | TxCommit <- txType = commit_

readAll :: (MonadIO m, ConcurrentMetaStoreM m, Key k, Value v)
        => ConcurrentDb k v
        -> m [(k, v)]
readAll = transactReadOnly Tree.toList

defaultConcurrentHandles :: ConcurrentHandles
defaultConcurrentHandles =
    ConcurrentHandles {
        concurrentHandlesMain      = "main.db"
      , concurrentHandlesMetadata1 = "meta.md1"
      , concurrentHandlesMetadata2 = "meta.md2"
      }

--------------------------------------------------------------------------------
