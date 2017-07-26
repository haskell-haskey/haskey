{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
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
import Data.Maybe (fromJust, isJust)
import qualified Data.Map as M

import System.Directory (removeDirectory, removeFile, getTemporaryDirectory)
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
    idb    <- run $ snd <$> create
    result <- run . runMaybeT $ foldlM (\(db, m) tx -> writeReadTest db tx m)
                                       (idb, M.empty)
                                       txs
    assert $ isJust result
  where

    writeReadTest :: Files String
                  -> TestTransaction Integer Integer
                  -> Map Integer Integer
                  -> MaybeT IO (Files String, Map Integer Integer)
    writeReadTest db tx m = do
        db'      <- openAndWrite db tx
        read'    <- openAndRead db'
        let expected = testTransactionResult m tx
        if read' == M.toList expected
            then return (db', expected)
            else error $ "error:"
                    ++ "\n    after:   " ++ show tx
                    ++ "\n    expectd: " ++ show (M.toList expected)
                    ++ "\n    got:     " ++ show read'

    create :: IO (Maybe (ConcurrentDb String Integer Integer), Files String)
    create = flip runStoreT emptyStore $ do
        openConcurrentHandles hnds
        createConcurrentDb hnds
      where
        hnds = defaultConcurrentHandles

    openAndRead db = fromJust <$> evalStoreT (open >>= readAll) db
    openAndWrite db tx = execStoreT (open >>= writeTransaction tx) db

    open = fromJust <$> openConcurrentDb defaultConcurrentHandles

--------------------------------------------------------------------------------

prop_file_backend :: PropertyM IO ()
prop_file_backend = forAllM genTestSequence $ \(TestSequence txs) -> do
    tmpDir <- run getTemporaryDirectory
    fp     <- run $ createTempDirectory tmpDir "db.haskey"
    let hnds = ConcurrentHandles {
        concurrentHandlesMain      = fp </> "main.db"
      , concurrentHandlesMetadata1 = fp </> "meta.md1"
      , concurrentHandlesMetadata2 = fp </> "meta.md2"
      }

    _      <- run $ create hnds
    result <- run . runMaybeT $ foldM (writeReadTest hnds)
                                      M.empty
                                      txs

    run $ removeFile (concurrentHandlesMain hnds)
    run $ removeFile (concurrentHandlesMetadata1 hnds)
    run $ removeFile (concurrentHandlesMetadata2 hnds)
    run $ removeDirectory fp

    assert $ isJust result
  where
    writeReadTest :: ConcurrentHandles FilePath
                  -> Map Integer Integer
                  -> TestTransaction Integer Integer
                  -> MaybeT IO (Map Integer Integer)
    writeReadTest hnds m tx = do
        _     <- lift $ openAndWrite hnds tx
        read' <- lift $ openAndRead hnds
        let expected = testTransactionResult m tx
        if read' == M.toList expected
            then return expected
            else error $ "error:"
                    ++ "\n    after:   " ++ show tx
                    ++ "\n    expectd: " ++ show (M.toList expected)
                    ++ "\n    got:     " ++ show read'

    create :: ConcurrentHandles FilePath
           -> IO (Maybe (ConcurrentDb FilePath Integer Integer), FS.Files FilePath)
    create hnds = flip FS.runStoreT FS.emptyStore $ do
        openConcurrentHandles hnds
        db <- createConcurrentDb hnds
        closeConcurrentHandles hnds
        return db

    openAndRead :: ConcurrentHandles FilePath
                -> IO [(Integer, Integer)]
    openAndRead hnds = fromJust <$> FS.evalStoreT (do
        db <- open hnds
        v  <- readAll db
        closeConcurrentHandles hnds
        return v)
        (FS.emptyStore :: FS.Files FilePath)

    openAndWrite :: ConcurrentHandles FilePath
                 -> TestTransaction Integer Integer
                 -> IO (FS.Files FilePath)
    openAndWrite hnds tx = flip FS.execStoreT FS.emptyStore $ do
        db  <- open hnds
        _   <- writeTransaction tx db
        closeConcurrentHandles hnds

    open hnds = fromJust <$> (openConcurrentHandles hnds >> openConcurrentDb hnds)

--------------------------------------------------------------------------------

writeTransaction :: (MonadIO m, ConcurrentMetaStoreM hnd m, Key k, Value v)
                 => TestTransaction k v
                 -> ConcurrentDb hnd k v
                 -> m (ConcurrentDb hnd k v)
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

readAll :: (MonadIO m, ConcurrentMetaStoreM hnd m, Key k, Value v)
        => ConcurrentDb hnd k v
        -> m [(k, v)]
readAll = transactReadOnly Tree.toList

defaultConcurrentHandles :: ConcurrentHandles String
defaultConcurrentHandles =
    ConcurrentHandles {
        concurrentHandlesMain      = "main.db"
      , concurrentHandlesMetadata1 = "meta.md1"
      , concurrentHandlesMetadata2 = "meta.md2"
      }

--------------------------------------------------------------------------------
