{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
module Integration.WriteOpenRead.Concurrent where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck
import Test.QuickCheck.Monadic
import Test.QuickCheck.Random

import Control.Applicative ((<$>))
import Control.Monad
import Control.Monad.Catch (MonadMask, Exception, throwM, catch)
import Control.Monad.IO.Class
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Maybe

import Data.Binary (Binary(..))
import Data.Foldable (foldlM)
import Data.Map (Map)
import Data.Maybe (isJust, fromJust, fromMaybe)
import Data.Typeable (Typeable)
import Data.Word (Word8)
import qualified Data.Map as M

import GHC.Generics (Generic)

import System.Directory (removeDirectoryRecursive,
                         getTemporaryDirectory, doesDirectoryExist,
                         writable, getPermissions)
import System.IO.Temp (createTempDirectory)

import Data.BTree.Alloc.Class
import Data.BTree.Alloc.Concurrent
import Data.BTree.Impure
import Data.BTree.Primitives
import Data.BTree.Store.File
import Data.BTree.Store.InMemory
import qualified Data.BTree.Impure as Tree

import Integration.WriteOpenRead.Transactions

tests :: Test
tests = testGroup "WriteOpenRead.Concurrent"
    [ testProperty "memory backend" (monadicIO prop_memory_backend)
    , testProperty "file backend" (monadicIO prop_file_backend)
    ]

case_bad_seed :: IO ()
case_bad_seed = do
    putStrLn "Testing bad case..."
    quickCheckWith args (monadicIO prop_memory_backend)
    putStrLn "    done"
  where
    -- This seed results in out of memory!!
    seed = 1576280407925194075
    gen = (mkQCGen seed, seed)
    args = stdArgs { replay = Just gen }

prop_memory_backend :: PropertyM IO ()
prop_memory_backend = forAllM (genTestSequence False) $ \(TestSequence txs) -> do
    (db, files) <- run create
    _ <- run $ foldlM (\(files', m) tx -> writeReadTest db files' tx m)
                      (files, M.empty)
                      txs
    return ()
  where

    writeReadTest :: ConcurrentDb Integer TestValue
                  -> MemoryFiles String
                  -> TestTransaction Integer TestValue
                  -> Map Integer TestValue
                  -> IO (MemoryFiles String, Map Integer TestValue)
    writeReadTest db files tx m = do
        files'   <- openAndWrite db files tx
        read'    <- openAndRead db files'
        let expected = fromMaybe m $ testTransactionResult m tx
        if read' == M.toList expected
            then return (files', expected)
            else error $ "error:"
                    ++ "\n    after:   " ++ show tx
                    ++ "\n    expectd: " ++ show (M.toList expected)
                    ++ "\n    got:     " ++ show read'

    create :: IO (ConcurrentDb Integer TestValue, MemoryFiles String)
    create = runMemoryStoreT m config emptyMemoryStore
      where
        m = do
            openConcurrentHandles hnds
            createConcurrentDb hnds
        hnds = concurrentHandles ""

    openAndRead db = evalMemoryStoreT (readAll db) config

    openAndWrite db files tx =
        execMemoryStoreT (writeTransaction tx db) config files

    config = fromJust $ memoryStoreConfigWithPageSize 256

--------------------------------------------------------------------------------

prop_file_backend :: PropertyM IO ()
prop_file_backend = forAllM (genTestSequence True) $ \(TestSequence txs) -> do
    exists <- run $ doesDirectoryExist "/var/run/shm"
    w      <- if exists then run $ writable <$> getPermissions "/var/run/shm"
                        else return False
    tmpDir <- if w then return "/var/run/shm"
                   else run getTemporaryDirectory
    fp     <- run $ createTempDirectory tmpDir "db.haskey"
    let hnds = concurrentHandles fp

    (db, files) <- run $ create hnds
    result <- run . runMaybeT $ foldM (writeReadTest db files)
                                      M.empty
                                      txs

    _ <- run $ runFileStoreT (closeConcurrentHandles hnds) config files

    run $ removeDirectoryRecursive fp

    assert $ isJust result
  where
    writeReadTest :: ConcurrentDb Integer TestValue
                  -> Files FilePath
                  -> Map Integer TestValue
                  -> TestTransaction Integer TestValue
                  -> MaybeT IO (Map Integer TestValue)
    writeReadTest db files m tx = do
        _     <- lift $ void (openAndWrite db files tx) `catch`
                            \TestException -> return ()
        read' <- lift $ openAndRead db files
        let expected = fromMaybe m $ testTransactionResult m tx
        if read' == M.toList expected
            then return expected
            else error $ "error:"
                    ++ "\n    after:   " ++ show tx
                    ++ "\n    expectd: " ++ show (M.toList expected)
                    ++ "\n    got:     " ++ show read'

    create :: ConcurrentHandles
           -> IO (ConcurrentDb Integer TestValue, Files FilePath)
    create hnds = runFileStoreT m config emptyFileStore
        where m = do openConcurrentHandles hnds
                     createConcurrentDb hnds

    openAndRead :: ConcurrentDb Integer TestValue
                -> Files FilePath
                -> IO [(Integer, TestValue)]
    openAndRead db = evalFileStoreT (readAll db) config

    openAndWrite :: ConcurrentDb Integer TestValue
                 -> Files FilePath
                 -> TestTransaction Integer TestValue
                 -> IO (Files FilePath)
    openAndWrite db files tx =
        execFileStoreT (void $ writeTransaction tx db) config files

    config = fromJust $ fileStoreConfigWithPageSize 256

--------------------------------------------------------------------------------

writeTransaction :: (MonadIO m, MonadMask m, ConcurrentMetaStoreM m, Key k, Value v)
                 => TestTransaction k v
                 -> ConcurrentDb k v
                 -> m ()
writeTransaction (TestTransaction txType actions) =
    transaction
  where
    writeAction (Insert k v)   = insertTree k v
    writeAction (Replace k v)  = insertTree k v
    writeAction (Delete k)     = deleteTree k
    writeAction ThrowException = const (throwM TestException)

    transaction = transact_ $
        foldl (>=>) return (map writeAction actions)
        >=> commitOrAbort

    commitOrAbort :: (AllocM n, MonadMask n) => Tree key val -> n (Transaction key val ())
    commitOrAbort
        | TxAbort  <- txType = const abort_
        | TxCommit <- txType = commit_

readAll :: (MonadIO m, MonadMask m, ConcurrentMetaStoreM m, Key k, Value v)
        => ConcurrentDb k v
        -> m [(k, v)]
readAll = transactReadOnly Tree.toList

--------------------------------------------------------------------------------

-- | Value used for testing.
--
-- This value will overflow 20% of the time.
newtype TestValue = TestValue (Either Integer [Word8])
                  deriving (Eq, Generic, Show, Typeable)

instance Binary TestValue where
instance Value TestValue where

instance Arbitrary TestValue where
    arbitrary =
        TestValue <$> frequency [(80, Left <$> small), (20, Right <$> big)]
      where
        small = arbitrary
        big = arbitrary

-- | Exception used for testing
data TestException = TestException deriving (Show, Typeable)

instance Exception TestException where

--------------------------------------------------------------------------------
