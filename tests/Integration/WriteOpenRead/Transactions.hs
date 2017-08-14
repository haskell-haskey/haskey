{-# LANGUAGE RecordWildCards #-}
module Integration.WriteOpenRead.Transactions where

import Test.QuickCheck

import Control.Applicative ((<$>), (<*>), pure)
import Control.Monad.State

import Data.Foldable (foldlM)
import Data.List (inits)
import Data.Map (Map)
import Data.Maybe (fromMaybe)
import qualified Data.Map as M

--------------------------------------------------------------------------------

newtype TestSequence k v = TestSequence [TestTransaction k v]
                         deriving (Show)

data TransactionSetup = TransactionSetup { sequenceInsertFrequency :: !Int
                                         , sequenceReplaceFrequency :: !Int
                                         , sequenceDeleteFrequency :: !Int
                                         , sequenceExceptionFrequency :: !Int }
                      deriving (Show)

deleteHeavySetup :: TransactionSetup
deleteHeavySetup = TransactionSetup { sequenceInsertFrequency = 35
                                    , sequenceReplaceFrequency = 20
                                    , sequenceDeleteFrequency = 45
                                    , sequenceExceptionFrequency = 5 }

insertHeavySetup :: TransactionSetup
insertHeavySetup = TransactionSetup { sequenceInsertFrequency = 12
                                    , sequenceReplaceFrequency = 4
                                    , sequenceDeleteFrequency = 4
                                    , sequenceExceptionFrequency = 1}

genTransactionSetup :: Gen TransactionSetup
genTransactionSetup = elements [deleteHeavySetup, insertHeavySetup]

data TxType = TxAbort | TxCommit
            deriving (Show)

genTxType :: Gen TxType
genTxType = elements [TxAbort, TxCommit]

data TestTransaction k v = TestTransaction TxType [TestAction k v]
                         deriving (Show)

testTransactionResult :: Ord k => Map k v -> TestTransaction k v -> Maybe (Map k v)
testTransactionResult m (TestTransaction TxAbort _) = Just m
testTransactionResult m (TestTransaction TxCommit actions)
    = foldlM (flip doAction) m actions

data TestAction k v = Insert k v
                    | Replace k v
                    | Delete k
                    | ThrowException
                    deriving (Show)

doAction :: Ord k => TestAction k v -> Map k v -> Maybe (Map k v)
doAction action m
    | Insert  k v    <- action = Just $ M.insert k v m
    | Replace k v    <- action = Just $ M.insert k v m
    | Delete  k      <- action = Just $ M.delete k m
    | ThrowException <- action = Nothing

genTestTransaction :: (Ord k, Arbitrary k, Arbitrary v) => Map k v -> TransactionSetup -> Gen (TestTransaction k v, Maybe (Map k v))
genTestTransaction db TransactionSetup{..} = sized $ \n -> do
    k            <- choose (0, n)
    (m, actions) <- execStateT (replicateM k next) (Just db, [])
    tx           <- TestTransaction <$> genTxType <*> pure (reverse actions)
    return (tx, m)
  where
    genAction :: (Ord k, Arbitrary k, Arbitrary v)
              => Maybe (Map k v)
              -> Gen (TestAction k v)
    genAction Nothing = genException
    genAction (Just m)
        | M.null m = genInsert
        | otherwise = frequency [(sequenceInsertFrequency,    genInsert   ),
                                 (sequenceReplaceFrequency,   genReplace m),
                                 (sequenceDeleteFrequency,    genDelete m ),
                                 (sequenceExceptionFrequency, genException)]

    genInsert :: (Arbitrary k, Arbitrary v) => Gen (TestAction k v)
    genInsert = Insert <$> arbitrary <*> arbitrary
    genReplace m = Replace <$> elements (M.keys m) <*> arbitrary
    genDelete m = Delete <$> elements (M.keys m)
    genException = return ThrowException

    next :: (Ord k, Arbitrary k, Arbitrary v)
         => StateT (Maybe (Map k v), [TestAction k v]) Gen ()
    next = do
        (m, actions) <- get
        action <- lift $ genAction m
        put (m >>= doAction action, action:actions)

shrinkTestTransaction :: (Ord k, Arbitrary k, Arbitrary v)
                   => TestTransaction k v
                   -> [TestTransaction k v]
shrinkTestTransaction (TestTransaction _ []) = []
shrinkTestTransaction (TestTransaction t actions) = map (TestTransaction t) (init (inits actions))

genTestSequence :: (Ord k, Arbitrary k, Arbitrary v) => Gen (TestSequence k v)
genTestSequence = sized $ \n -> do
    k <- choose (0, n)
    (_, txs) <- execStateT (replicateM k next) (M.empty, [])
    return $ TestSequence (reverse txs)
  where
    next :: (Ord k, Arbitrary k, Arbitrary v)
         => StateT (Map k v, [TestTransaction k v]) Gen ()
    next = do
        (m, txs) <- get
        (tx, m') <- lift $ genTransactionSetup >>= genTestTransaction m
        put (fromMaybe m m', tx:txs)

shrinkTestSequence :: (Ord k, Arbitrary k, Arbitrary v)
                   => TestSequence k v
                   -> [TestSequence k v]
shrinkTestSequence (TestSequence txs) = map TestSequence (shrinkList shrinkTestTransaction txs)
