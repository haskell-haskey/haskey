{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
module Integration.WriteOpenRead where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck
import Test.QuickCheck.Monadic

import Control.Applicative ((<$>), (<*>))
import Control.Monad
import Control.Monad.Identity
import Control.Monad.State

import Data.List (inits)
import Data.Map (Map)
import qualified Data.Map as M

import System.Directory (removeFile, getTemporaryDirectory)
import System.IO
import System.IO.Temp (openTempFile)

import Data.BTree.Alloc.Append
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
prop_memory_backend = forAll genTransactionSetup $ \setup ->
                      forAllShrink (genTestTransaction setup)
                                   shrinkTestTransaction $ \testSeq ->
    let Just (files, orig) = createAndWriteMemory testSeq
        Just read'         = openAndReadMemory files
    in
    read' == M.toList orig

createAndWriteMemory :: TestTransaction Integer Integer
                     -> Maybe (Files String, Map Integer Integer)
createAndWriteMemory testSeq =
    let (db, files) = runIdentity . flip runStoreT initialStore $
                        createAppendDb "Main" >>= writeSequence testSeq
    in
    case db of
        Nothing -> Nothing
        Just _-> Just (files, testTransactionResult testSeq)

  where
    initialStore :: Files String
    initialStore = M.fromList [("Main", M.empty)]

openAndReadMemory :: Files String
                  -> Maybe [(Integer, Integer)]
openAndReadMemory files =
    runIdentity . flip evalStoreT files $ do
        Just db <- openAppendDb "Main"
        transactReadOnly Tree.toList db

--------------------------------------------------------------------------------

prop_file_backend :: PropertyM IO ()
prop_file_backend = do
    tmpDir   <- run getTemporaryDirectory
    (fp, fh) <- run $ openTempFile tmpDir "db.haskey"

    Just orig  <- createAndWriteFile fp fh
    Just read' <- openAndReadFile fp fh

    run $ hClose fh
    run $ removeFile fp

    assert $ read' == M.toList orig

createAndWriteFile :: FilePath
                   -> Handle
                   -> PropertyM IO (Maybe (Map Integer Integer))
createAndWriteFile fp fh = forAllM genTransactionSetup $ \setup ->
                           forAllM (genTestTransaction setup) $ \testSeq -> run $ do
    (db, _) <- FS.runStore fp fh $
                createAppendDb fp >>= writeSequence testSeq
    case db of
        Nothing -> return Nothing
        Just _ -> return $ Just (testTransactionResult testSeq)

openAndReadFile :: FilePath
                -> Handle
                -> PropertyM IO (Maybe [(Integer, Integer)])
openAndReadFile fp fh = run $
    FS.evalStore fp fh $ do
        Just db <- openAppendDb fp
        transactReadOnly Tree.toList db

--------------------------------------------------------------------------------

writeSequence :: (AppendMetaStoreM hnd m, Key k, Value v)
              => TestTransaction k v
              -> AppendDb hnd k v
              -> m (AppendDb hnd k v)
writeSequence (TestTransaction actions) =
    transaction
  where
    writeAction (Insert k v)  = insertTree k v
    writeAction (Replace k v) = insertTree k v
    writeAction (Delete k)    = deleteTree k

    transaction = transact_ $
        foldl (>=>) return (map writeAction actions)
        >=> commit_

--------------------------------------------------------------------------------

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

newtype TestTransaction k v = TestTransaction [TestAction k v]
                            deriving (Show)

testTransactionResult :: Ord k => TestTransaction k v -> Map k v
testTransactionResult (TestTransaction actions) = foldl doAction M.empty actions

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
    return $ TestTransaction (reverse actions)
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
shrinkTestTransaction (TestTransaction []) = []
shrinkTestTransaction (TestTransaction actions) = map TestTransaction (init (inits actions))
