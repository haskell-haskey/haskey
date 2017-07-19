{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
module Integration.WriteOpenRead where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck
import Test.QuickCheck.Monadic

import Control.Applicative ((<$>), (<*>), pure)
import Control.Monad
import Control.Monad.Identity
import Control.Monad.State
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Maybe

import Data.Foldable (foldlM)
import Data.List (inits)
import Data.Map (Map)
import Data.Maybe (fromJust, isJust)
import qualified Data.Map as M

import System.Directory (removeFile, getTemporaryDirectory)
import System.IO
import System.IO.Temp (openTempFile)

import Data.BTree.Alloc.Append
import Data.BTree.Alloc.Class (AllocM)
import Data.BTree.Impure
import Data.BTree.Primitives
import Data.BTree.Store.Binary
import qualified Data.BTree.Impure as Tree
import qualified Data.BTree.Store.File as FS

tests :: Test
tests = testGroup "WriteOpenRead"
    [ testProperty "memory backend" prop_memory_backend
    , testProperty "file backend" (monadicIO prop_file_backend)
    ]

prop_memory_backend :: Property
prop_memory_backend = forAllShrink genTestSequence
                                   shrinkTestSequence $ \(TestSequence txs) ->
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

    create :: (Maybe (AppendDb String Integer Integer), Files String)
    create = runIdentity $ runStoreT (createAppendDb "Main")
                                     (emptyStore "Main")

    openAndRead db = fromJust . runIdentity $ evalStoreT (open >>= readAll) db
    openAndWrite db tx = runIdentity $ execStoreT (open >>= writeTransaction tx) db

    open = fromJust <$> openAppendDb "Main"


--------------------------------------------------------------------------------

prop_file_backend :: PropertyM IO ()
prop_file_backend = forAllM genTestSequence $ \(TestSequence txs) -> do
    tmpDir   <- run getTemporaryDirectory
    (fp, fh) <- run $ openTempFile tmpDir "db.haskey"

    Just _ <- run $ create fp fh
    result <- run . runMaybeT $ foldM (flip (writeReadTest fp fh))
                                      M.empty
                                      txs

    run $ hClose fh
    run $ removeFile fp

    assert $ isJust result
  where
    writeReadTest :: FilePath
                  -> Handle
                  -> TestTransaction Integer Integer
                  -> Map Integer Integer
                  -> MaybeT IO (Map Integer Integer)
    writeReadTest fp fh tx m = do
        lift $ openAndWrite fp fh tx
        read' <- lift $ openAndRead fp fh
        let expected = testTransactionResult m tx
        if read' == M.toList expected
            then return expected
            else error $ "error:"
                    ++ "\n    after:   " ++ show tx
                    ++ "\n    expectd: " ++ show (M.toList expected)
                    ++ "\n    got:     " ++ show read'

    create :: FilePath
           -> Handle
           -> IO (Maybe (AppendDb FilePath Integer Integer))
    create fp fh = FS.evalStore fp fh (createAppendDb fp)


    openAndRead fp fh = fromJust <$> FS.evalStore fp fh (open fp >>= readAll)
    openAndWrite fp fh tx = void $ FS.evalStore fp fh (open fp >>= writeTransaction tx)
    open fp = fromJust <$> openAppendDb fp

--------------------------------------------------------------------------------

writeTransaction :: (AppendMetaStoreM hnd m, Key k, Value v)
                 => TestTransaction k v
                 -> AppendDb hnd k v
                 -> m (AppendDb hnd k v)
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

readAll :: (AppendMetaStoreM hnd m, Key k, Value v)
        => AppendDb hnd k v
        -> m [(k, v)]
readAll = transactReadOnly Tree.toList

--------------------------------------------------------------------------------

newtype TestSequence k v = TestSequence [TestTransaction k v]
                         deriving (Show)

data TransactionSetup = TransactionSetup { sequenceInsertFrequency :: !Int
                                         , sequenceReplaceFrequency :: !Int
                                         , sequenceDeleteFrequency :: !Int }
                      deriving (Show)

deleteHeavySetup :: TransactionSetup
deleteHeavySetup = TransactionSetup { sequenceInsertFrequency = 35
                                    , sequenceReplaceFrequency = 20
                                    , sequenceDeleteFrequency = 45 }

insertHeavySetup :: TransactionSetup
insertHeavySetup = TransactionSetup { sequenceInsertFrequency = 6
                                    , sequenceReplaceFrequency = 2
                                    , sequenceDeleteFrequency = 2 }

genTransactionSetup :: Gen TransactionSetup
genTransactionSetup = elements [deleteHeavySetup, insertHeavySetup]

data TxType = TxAbort | TxCommit
            deriving (Show)

genTxType :: Gen TxType
genTxType = elements [TxAbort, TxCommit]

data TestTransaction k v = TestTransaction TxType [TestAction k v]
                         deriving (Show)

testTransactionResult :: Ord k => Map k v -> TestTransaction k v -> Map k v
testTransactionResult m (TestTransaction TxAbort _) = m
testTransactionResult m (TestTransaction TxCommit actions)
    = foldl doAction m actions

data TestAction k v = Insert k v
                    | Replace k v
                    | Delete k
                    deriving (Show)

doAction :: Ord k => Map k v -> TestAction k v -> Map k v
doAction m action
    | Insert  k v <- action = M.insert k v m
    | Replace k v <- action = M.insert k v m
    | Delete  k   <- action = M.delete k m

genTestTransaction :: (Ord k, Arbitrary k, Arbitrary v) => TransactionSetup -> Gen (TestTransaction k v)
genTestTransaction TransactionSetup{..} = sized $ \n -> do
    k            <- choose (0, n)
    (_, actions) <- execStateT (replicateM k next) (M.empty, [])
    TestTransaction <$> genTxType <*> pure (reverse actions)
  where
    genAction :: (Ord k, Arbitrary k, Arbitrary v)
              => Map k v
              -> Gen (TestAction k v)
    genAction m
        | M.null m = genInsert
        | otherwise = frequency [(sequenceInsertFrequency,  genInsert   ),
                                 (sequenceReplaceFrequency, genReplace m),
                                 (sequenceDeleteFrequency,  genDelete m )]

    genInsert :: (Arbitrary k, Arbitrary v) => Gen (TestAction k v)
    genInsert = Insert <$> arbitrary <*> arbitrary
    genReplace m = Replace <$> elements (M.keys m) <*> arbitrary
    genDelete m = Delete <$> elements (M.keys m)

    next :: (Ord k, Arbitrary k, Arbitrary v)
         => StateT (Map k v, [TestAction k v]) Gen ()
    next = do
        (m, actions) <- get
        action <- lift $ genAction m
        put (doAction m action, action:actions)

shrinkTestTransaction :: (Ord k, Arbitrary k, Arbitrary v)
                   => TestTransaction k v
                   -> [TestTransaction k v]
shrinkTestTransaction (TestTransaction _ []) = []
shrinkTestTransaction (TestTransaction t actions) = map (TestTransaction t) (init (inits actions))

genTestSequence :: (Ord k, Arbitrary k, Arbitrary v) => Gen (TestSequence k v)
genTestSequence = TestSequence <$> listOf (genTransactionSetup >>= genTestTransaction)

shrinkTestSequence :: (Ord k, Arbitrary k, Arbitrary v)
                   => TestSequence k v
                   -> [TestSequence k v]
shrinkTestSequence (TestSequence txs) = map TestSequence (shrinkList shrinkTestTransaction txs)
