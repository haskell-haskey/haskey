{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE StandaloneDeriving #-}
-- | Non empty wrapper around the impure 'Tree'.
module Data.BTree.Impure.NonEmpty (
  -- * Structures
  NonEmptyTree(..)
, Node(..)

  -- * Conversions
, fromTree
, toTree
, nonEmptyToList

  -- * Construction
, fromNonEmptyList

  -- * Inserting
, insertNonEmptyTree
, insertNonEmptyTreeMany
) where

import Control.Applicative ((<$>), (<*>))

import Data.Binary
import Data.List.NonEmpty (NonEmpty((:|)))
import Data.Map (Map)
import Data.Maybe (fromJust)
import Data.Typeable (Typeable)
import qualified Data.List.NonEmpty as NE
import qualified Data.Map as M

import Data.BTree.Alloc.Class
import Data.BTree.Impure (Tree(..), Node(..), insertTree, insertTreeMany, empty, toList)
import Data.BTree.Primitives

-- | A non-empty variant of 'Tree'.
data NonEmptyTree key val where
    NonEmptyTree :: { -- | A term-level witness for the type-level height index.
                      treeHeight :: Height height
                    , -- | An empty tree is represented by 'Nothing'. Otherwise it's
                      --   'Just' a 'NodeId' pointer the root.
                      treeRootId :: NodeId height key val
                    } -> NonEmptyTree key val
    deriving (Typeable)

deriving instance (Show key, Show val) => Show (NonEmptyTree key val)

instance (Value k, Value v) => Value (NonEmptyTree k v) where

instance Binary (NonEmptyTree key val) where
    put (NonEmptyTree h root) = put h >> put root
    get = NonEmptyTree <$> get <*> get

-- | Convert a 'Tree' into a 'NonEmptyTree'.
fromTree :: Tree key val -> Maybe (NonEmptyTree key val)
fromTree (Tree h n) = case n of
    Nothing   -> Nothing
    Just root -> Just $ NonEmptyTree h root

-- | Convert a 'NonEmptyTree' into a 'Tree'.
toTree :: NonEmptyTree key val -> Tree key val
toTree (NonEmptyTree h n) = Tree h (Just n)

-- | Create a 'NonEmptyTree' from a 'NonEmpty' list.
fromNonEmptyList :: (AllocM m, Key k, Value v)
                 => NonEmpty (k, v)
                 -> m (NonEmptyTree k v)
fromNonEmptyList (x :| xs) = fromJust . fromTree <$> insertTreeMany (M.fromList (x:xs)) empty

-- | Insert an item into a 'NonEmptyTree'
insertNonEmptyTree :: (AllocM m, Key k, Value v)
                   => k
                   -> v
                   -> NonEmptyTree k v
                   -> m (NonEmptyTree k v)
insertNonEmptyTree k v tree = fromJust . fromTree <$> insertTree k v (toTree tree)

-- | Bulk insert a bunch of key-value pairs into a 'NonEmptyTree'.
insertNonEmptyTreeMany :: (AllocM m, Key k, Value v)
                       => Map k v
                       -> NonEmptyTree k v
                       -> m (NonEmptyTree k v)
insertNonEmptyTreeMany kvs tree = fromJust . fromTree <$> insertTreeMany kvs (toTree tree)

-- | Convert a non-empty tree to a list of key-value pairs.
nonEmptyToList :: (AllocReaderM m, Key k, Value v)
               => NonEmptyTree k v
               -> m (NonEmpty (k, v))
nonEmptyToList tree = NE.fromList <$> toList (toTree tree)
