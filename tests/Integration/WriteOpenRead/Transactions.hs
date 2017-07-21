{-# LANGUAGE RecordWildCards #-}
module Integration.WriteOpenRead.Transactions where

import Test.QuickCheck

import Control.Applicative ((<$>), (<*>), pure)
import Control.Monad.State

import Data.List (inits)
import Data.Map (Map)
import qualified Data.Map as M

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
