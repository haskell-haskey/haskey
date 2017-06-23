{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}

module Data.BTree.Primitives
  ( module Data.BTree.Primitives.Height
  , module Data.BTree.Primitives.Ids
  , module Data.BTree.Primitives.Index
  , module Data.BTree.Primitives.Key
  , module Data.BTree.Primitives.Leaf
  , module Data.BTree.Primitives.Value
  , module Data.BTree.Primitives
  ) where

import Control.Applicative ((<$>))

import Data.BTree.Primitives.Height
import Data.BTree.Primitives.Ids
import Data.BTree.Primitives.Index
import Data.BTree.Primitives.Key
import Data.BTree.Primitives.Leaf
import Data.BTree.Primitives.Value

import Data.Binary (Binary(..))
import Data.Map (Map)
import Data.Proxy (Proxy(..))
import Data.Typeable (Typeable, typeRep)

import GHC.Generics (Generic)

import Unsafe.Coerce

--------------------------------------------------------------------------------

{-| A node in a B+-tree.

    Nodes are parameterized over the key and value types and are additionally
    indexed by their height. All paths from the root to the leaves have the same
    length. The height is the number of edges from the root to the leaves,
    i.e. leaves are at height zero and index nodes increase the height.

    Sub-trees are represented by a 'NodeId' that are used to resolve the actual
    storage location of the sub-tree node.
-}
data Node height key val where
    Idx  :: { idxChildren      ::  Index key (NodeId height key val)
            } -> Node ('S height) key val
    Leaf :: { leafItems        ::  Map key val
            } -> Node 'Z key val
    deriving (Typeable)

instance (Eq key, Eq val) => Eq (Node 'Z key val) where
    Leaf x == Leaf y = x == y

instance (Eq key, Eq val) => Eq (Node ('S h) key val) where
    Idx x == Idx y = x == y

data BNode = BIdx | BLeaf deriving Generic
instance Binary BNode where

instance (Binary key, Binary val) => Binary (Node 'Z key val) where
    put (Leaf items) = put BLeaf >> put items
    get = get >>= \case BLeaf -> Leaf <$> get
                        BIdx  -> fail "expected a leaf node, but found an idx node"

instance (Binary key, Binary val) => Binary (Node ('S height) key val) where
    put (Idx idx) = put BIdx >> put idx
    get = get >>= \case BIdx -> Idx <$> get
                        BLeaf -> fail "expected an idx node, but found a leaf node"

{-| A B+-tree.

    This is a simple wrapper around a root 'Node'. The type-level height is
    existentially quantified, but a term-level witness is stores.
-}
data Tree key val where
    Tree :: { -- | A term-level witness for the type-level height index.
              treeHeight :: Height height
            , -- | An empty tree is represented by 'Nothing'. Otherwise it's
              --   'Just' a 'NodeId' pointer the root.
              treeRootId :: Maybe (NodeId height key val)
            } -> Tree key val

deriving instance (Show key, Show val) => Show (Node height key val)
deriving instance (Show key, Show val) => Show (Tree key val)

{-| Create an empty tree. -}
empty :: Tree k v
empty = Tree zeroHeight Nothing

{-| Cast a node to a different type.

    Essentially this is just a drop-in replacement for 'Data.Typeable.cast'.
-}
castNode :: forall n key1 val1 height1 key2 val2 height2.
       (Typeable key1, Typeable val1, Typeable key2, Typeable val2)
    => Height height1      -- ^ Term-level witness for the source height.
    -> Height height2      -- ^ Term-level witness for the target height.
    -> n height1 key1 val1 -- ^ Node to cast.
    -> Maybe (n height2 key2 val2)
castNode height1 height2 n
    | typeRep (Proxy :: Proxy key1) == typeRep (Proxy :: Proxy key2)
    , typeRep (Proxy :: Proxy val1) == typeRep (Proxy :: Proxy val2)
    , fromHeight height1 == fromHeight height2
    = Just (unsafeCoerce n)
    | otherwise
    = Nothing

--------------------------------------------------------------------------------
