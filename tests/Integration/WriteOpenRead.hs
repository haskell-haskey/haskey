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
prop_memory_backend = forAll genSequencySetup $ \setup ->
                      forAll (genTestSequence setup) $ \testSeq ->
    let Just (files, orig) = createAndWriteMemory testSeq
        Just read'         = openAndReadMemory files
    in
    read' == M.toList orig

createAndWriteMemory :: TestSequence Integer Integer
                     -> Maybe (Files String, Map Integer Integer)
createAndWriteMemory testSeq =
    let (db, files) = runIdentity . flip runStoreT initialStore $
                        createAppendDb "Main" >>= writeSequence testSeq
    in
    case db of
        Nothing -> Nothing
        Just _-> Just (files, testSequenceResult testSeq)

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
createAndWriteFile fp fh = forAllM genSequencySetup $ \setup ->
                           forAllM (genTestSequence setup) $ \testSeq -> run $ do
    (db, _) <- FS.runStore fp fh $
                createAppendDb fp >>= writeSequence testSeq
    case db of
        Nothing -> return Nothing
        Just _ -> return $ Just (testSequenceResult testSeq)

openAndReadFile :: FilePath
                -> Handle
                -> PropertyM IO (Maybe [(Integer, Integer)])
openAndReadFile fp fh = run $
    FS.evalStore fp fh $ do
        Just db <- openAppendDb fp
        transactReadOnly Tree.toList db

--------------------------------------------------------------------------------

writeSequence :: (AppendMetaStoreM hnd m, Key k, Value v)
              => TestSequence k v
              -> AppendDb hnd k v
              -> m (AppendDb hnd k v)
writeSequence (TestSequence _ actions) =
    transaction
  where
    writeAction (Insert k v)  = insertTree k v
    writeAction (Replace k v) = insertTree k v
    writeAction (Delete k)    = deleteTree k

    transaction = transact_ $
        foldl (>=>) return (map writeAction actions)
        >=> commit_

--------------------------------------------------------------------------------

data SequenceSetup = SequenceSetup { sequenceInsertFrequency :: !Int
                                   , sequenceReplaceFrequency :: !Int
                                   , sequenceDeleteFrequency :: !Int }
                   deriving (Show)

deleteHeavySetup :: SequenceSetup
deleteHeavySetup = SequenceSetup { sequenceInsertFrequency = 35
                                 , sequenceReplaceFrequency = 20
                                 , sequenceDeleteFrequency = 45 }

insertHeavySetup :: SequenceSetup
insertHeavySetup = SequenceSetup { sequenceInsertFrequency = 6
                                 , sequenceReplaceFrequency = 2
                                 , sequenceDeleteFrequency = 2 }

genSequencySetup :: Gen SequenceSetup
genSequencySetup = elements [deleteHeavySetup, insertHeavySetup]

data TestSequence k v = TestSequence (Map k v) [TestAction k v]
                      deriving (Show)

testSequenceResult :: TestSequence k v -> Map k v
testSequenceResult (TestSequence m _) = m

data TestAction k v = Insert k v
                    | Replace k v
                    | Delete k
                    deriving (Show)

genTestSequence :: (Ord k, Arbitrary k, Arbitrary v) => SequenceSetup -> Gen (TestSequence k v)
genTestSequence SequenceSetup{..} = sized $ \n -> do
    k            <- choose (0, n)
    (m, actions) <- execStateT (replicateM k next) (M.empty, [])
    return $ TestSequence m (reverse actions)
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

    doAction m action
        | Insert  k v <- action = M.insert k v m
        | Replace k v <- action = M.insert k v m
        | Delete  k   <- action = M.delete k m
