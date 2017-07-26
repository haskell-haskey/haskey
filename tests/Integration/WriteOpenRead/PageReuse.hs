{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
module Integration.WriteOpenRead.PageReuse where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck
import Test.QuickCheck.Monadic

import Control.Applicative ((<$>))
import Control.Monad
import Control.Monad.Identity
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Maybe

import Data.Foldable (foldlM)
import Data.Map (Map)
import Data.Maybe (fromJust, isJust)
import qualified Data.Map as M

import System.Directory (removeFile, getTemporaryDirectory)
import System.IO
import System.IO.Temp (openTempFile)

import Data.BTree.Alloc.Class
import Data.BTree.Alloc.PageReuse
import Data.BTree.Impure
import Data.BTree.Primitives
import Data.BTree.Store.Class
import Data.BTree.Store.Binary
import qualified Data.BTree.Impure as Tree
import qualified Data.BTree.Store.File as FS

import Integration.WriteOpenRead.Transactions

tests :: Test
tests = testGroup "WriteOpenRead.PageReuse"
    [ testProperty "memory backend" prop_memory_backend
    , testProperty "file backend" (monadicIO prop_file_backend)
    ]

prop_memory_backend :: Property
prop_memory_backend = forAll genTestSequence $ \(TestSequence txs) ->
    let (_, idb) = create
        result   = foldlM (\(db, m) tx -> writeReadTest db tx m)
                          (idb, M.empty)
                          txs
    in case result of
        Nothing -> False
        Just _  -> True
  where

    writeReadTest :: Files String
                  -> TestTransaction Integer Integer
                  -> Map Integer Integer
                  -> Maybe (Files String, Map Integer Integer)
    writeReadTest db tx m =
        let db'      = openAndWrite db tx
            read'    = openAndRead db'
            expected = testTransactionResult m tx
        in if read' == M.toList expected
            then Just (db', expected)
            else error $ "error:"
                    ++ "\n    after:   " ++ show tx
                    ++ "\n    expectd: " ++ show (M.toList expected)
                    ++ "\n    got:     " ++ show read'

    create :: (Maybe (PageReuseDb String Integer Integer), Files String)
    create = runIdentity $ runStoreT (openHandle "Main" >> createPageReuseDb "Main")
                                     emptyStore

    openAndRead db = fromJust . runIdentity $ evalStoreT (open >>= readAll) db
    openAndWrite db tx = runIdentity $ execStoreT (open >>= writeTransaction tx) db

    open = fromJust <$> openPageReuseDb "Main"


--------------------------------------------------------------------------------

prop_file_backend :: PropertyM IO ()
prop_file_backend = forAllM genTestSequence $ \(TestSequence txs) -> do
    tmpDir   <- run getTemporaryDirectory
    (fp, fh) <- run $ openTempFile tmpDir "db.haskey"
    run $ hClose fh

    fs     <- run $ snd <$> create fp
    result <- run . runMaybeT $ foldM (writeReadTest fp)
                                      (M.empty, fs)
                                      txs

    run $ close fp fs
    run $ removeFile fp

    assert $ isJust result
  where
    writeReadTest :: FilePath
                  -> (Map Integer Integer, FS.Files FilePath)
                  -> TestTransaction Integer Integer
                  -> MaybeT IO (Map Integer Integer, FS.Files FilePath)
    writeReadTest fp (m, fs) tx = do
        fs'   <- lift $ openAndWrite fp fs tx
        read' <- lift $ openAndRead fp fs'
        let expected = testTransactionResult m tx
        if read' == M.toList expected
            then return (expected, fs')
            else error $ "error:"
                    ++ "\n    after:   " ++ show tx
                    ++ "\n    expectd: " ++ show (M.toList expected)
                    ++ "\n    got:     " ++ show read'

    create :: FilePath
           -> IO (Maybe (PageReuseDb FilePath Integer Integer), FS.Files FilePath)
    create fp = FS.runStoreT (openHandle fp >> createPageReuseDb fp) FS.emptyStore

    close :: FilePath -> FS.Files FilePath -> IO ()
    close fp fs = void $ FS.runStoreT (closeHandle fp) fs


    openAndRead fp fs = fromJust <$> FS.evalStoreT (open fp >>= readAll) fs
    openAndWrite fp fs tx = FS.execStoreT (open fp >>= writeTransaction tx) fs
    open fp = fromJust <$> openPageReuseDb fp

--------------------------------------------------------------------------------

writeTransaction :: (PageReuseMetaStoreM hnd m, Key k, Value v)
                 => TestTransaction k v
                 -> PageReuseDb hnd k v
                 -> m (PageReuseDb hnd k v)
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

readAll :: (PageReuseMetaStoreM hnd m, Key k, Value v)
        => PageReuseDb hnd k v
        -> m [(k, v)]
readAll = transactReadOnly Tree.toList

--------------------------------------------------------------------------------
