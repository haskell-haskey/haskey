{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
module Main where

import Control.Monad
import Control.Monad.Identity
import Control.Monad.State

import Data.Map (Map)
import qualified Data.Map as M

import System.Directory (removeFile)
import System.IO
import System.IO.Temp (emptySystemTempFile)

import Test.QuickCheck
import Test.QuickCheck.Monadic

import Text.PrettyPrint
import Text.Show.Pretty hiding (Value)

import Data.BTree.Alloc.Append
import Data.BTree.Store.Binary
import Data.BTree.Insert
import Data.BTree.Delete
import Data.BTree.Primitives
import qualified Data.BTree.Fold as Tree
import qualified Data.BTree.Store.File as FS

--------------------------------------------------------------------------------

test :: (Maybe (AppendDb String Integer Integer), Files String)
test = runIdentity . flip runStoreT initialStore $
    createAppendDb "Main"
    >>= transact
        (   insertTree 1 0x0
        >=> insertTree 2 0x1
        >=> insertTree 4 0x2
        >=> insertTree 5 0x3
        >=> deleteTree 2
        >=> insertTree 4 0x4
        >=> insertTree 3 0x5
        >=> deleteTree 1
        >=> deleteTree 3
        >=> deleteTree 4
        >=> deleteTree 5
        )
  where
    initialStore :: Files String
    initialStore = M.fromList [ ("Main", M.empty) ]

main :: IO ()
main = do
    putStrLn "In-memory:"
    print' test
    putStrLn (renderStyle (style {lineLength=260}) (ppDoc test))

    putStrLn "File:"
    quickCheck $ monadicIO testFile
  where
    print' :: Show a => a -> IO ()
    print' = putStrLn . renderStyle (style {lineLength=260}) . ppDoc

--------------------------------------------------------------------------------

testFile :: PropertyM IO Bool
testFile = do
    fp <- run $ emptySystemTempFile "db.haskey"
    fh <- run $ openFile fp ReadWriteMode

    Just orig  <- createAndWriteFile fp fh
    Just read' <- openAndReadFile fp fh

    run $ hClose fh
    run $ removeFile fp

    return $ read' == M.toList orig

createAndWriteFile :: FilePath
                   -> Handle
                   -> PropertyM IO (Maybe (Map Integer Integer))
createAndWriteFile fp fh = forAllM genTestSequence $ \testSeq -> run $ do
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
        readTransact Tree.toList db

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

    transaction = transact $ foldl (>=>) return (map writeAction actions)

--------------------------------------------------------------------------------

data TestSequence k v = TestSequence (Map k v) [TestAction k v]
                      deriving (Show)

testSequenceResult :: TestSequence k v -> Map k v
testSequenceResult (TestSequence m _) = m

data TestAction k v = Insert k v
                    | Replace k v
                    | Delete k
                    deriving (Show)

genTestSequence :: (Ord k, Arbitrary k, Arbitrary v) => Gen (TestSequence k v)
genTestSequence = sized $ \n -> do
    k            <- choose (0, n)
    (m, actions) <- execStateT (replicateM k next) (M.empty, [])
    return $ TestSequence m (reverse actions)
  where
    genAction :: (Ord k, Arbitrary k, Arbitrary v)
              => Map k v
              -> Gen (TestAction k v)
    genAction m
        | M.null m = genInsert
        | otherwise = oneof [genInsert, genReplace m, genDelete m]

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
