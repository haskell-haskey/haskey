module Properties.Impure.Fold where

import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck
import Test.QuickCheck.Monadic

import Control.Monad ((>=>))

import Data.Int
import qualified Data.Map as M

import Data.BTree.Alloc.Concurrent
import Data.BTree.Impure.Insert
import Data.BTree.Primitives
import Data.BTree.Store.Binary
import qualified Data.BTree.Impure.Fold as Tree

tests :: Test
tests = testGroup "Impure.Fold"
    [ testProperty "foldable toList fromList" (monadicIO $ forAllM arbitrary prop_foldable_toList_fromList)
    ]

prop_foldable_toList_fromList :: [(Int64, Integer)] -> PropertyM IO ()
prop_foldable_toList_fromList kvs = do
    v <- evalStore $ do
        db <- createConcurrentDb hnds
        insertAll kvs db
        transactReadOnly Tree.toList db
    case v of
        Just l -> assert $ l == M.toList (M.fromList kvs)
        Nothing -> assert False
  where
    insertAll kvs = transact_ $
        foldl (>=>) return (map (uncurry insertTree) kvs)
        >=> commit_

    hnds = ConcurrentHandles {
        concurrentHandlesMain = "main"
      , concurrentHandlesMetadata1 = "meta1"
      , concurrentHandlesMetadata2 = "meta2"
      }

    evalStore action = evalStoreT (openConcurrentHandles hnds >> action) emptyStore
