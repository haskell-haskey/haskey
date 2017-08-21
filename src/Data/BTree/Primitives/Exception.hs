{-# LANGUAGE DeriveDataTypeable #-}
-- | A collection of exceptions that can be raised in the pure algorithms in
-- "Data.BTree.Primitives", "Data.BTree.Impure" and "Data.BTree.Pure".
module Data.BTree.Primitives.Exception (
  -- * Re-exports
  throw

  -- * Custom exceptions
, TreeAlgorithmError(..)
, KeyTooLargeError(..)
) where

import Control.Exception (Exception, throw)

import Data.Typeable (Typeable)

-- | An exception raised when the pure modification algorithms are called using
-- invalid state.
--
-- This exception is only raised when a the library contains a bug.
--
-- The first argument is a function name indicating the location of the error.
-- The second argument is the description of the error.
data TreeAlgorithmError = TreeAlgorithmError String String deriving (Typeable)

instance Show TreeAlgorithmError where
    show (TreeAlgorithmError loc msg) =
        loc ++ ": " ++ msg ++
        " (TreeAlgorithmError, this indicates a " ++
        "bug in the haskey-btree library, please report)"

instance Exception TreeAlgorithmError where

-- | An exception thrown when the keys inserted in the database are larger than
-- 'Data.BTree.Alloc.Class.maxKeySize'.
--
-- Note that this exception can be thrown long after the key violating the
-- maximum key size was inserted. It is only detected when the tree
-- modification algorithms try to split the node containing that key.
--
-- Increase the page size to fix this problem.
data KeyTooLargeError = KeyTooLargeError deriving (Show, Typeable)

instance Exception KeyTooLargeError where
