module Properties.Impure.Insert where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck
import Test.QuickCheck.Monadic

import Control.Monad ((>=>))

import Data.Int
import Data.Maybe (isJust)
import qualified Data.Map as M

import Data.BTree.Alloc.Concurrent
import Data.BTree.Impure.Insert
import Data.BTree.Store.Binary
import qualified Data.BTree.Impure.Fold as Tree

tests :: Test
tests = testGroup "Impure.Insert"
    [ testProperty "insertTreeMany" (monadicIO $ forAllM arbitrary $ uncurry prop_insertTreeMany)
    ]

prop_insertTreeMany :: [(Int64, Integer)] -> [(Int64, Integer)] -> PropertyM IO ()
prop_insertTreeMany xs ys = do
    ty1' <- ty1
    ty2' <- ty2
    assert $ ty1' == ty2' && isJust ty1'
  where
    tx  = do
        db <- createConcurrentDb hnds
        insertAll xs db
        return db
    ty1 = evalStore $ do
        db <- tx
        insertAll ys db
        transactReadOnly Tree.toList db
    ty2 = evalStore $ do
        db <- tx
        transact_ (insertTreeMany (M.fromList ys) >=> commit_) db
        transactReadOnly Tree.toList db

    insertAll kvs = transact_ $
        foldl (>=>) return (map (uncurry insertTree) kvs)
        >=> commit_

    hnds = ConcurrentHandles {
        concurrentHandlesMain = "main"
      , concurrentHandlesMetadata1 = "meta1"
      , concurrentHandlesMetadata2 = "meta2"
      }

    evalStore action = evalStoreT (openConcurrentHandles hnds >> action) emptyStore
