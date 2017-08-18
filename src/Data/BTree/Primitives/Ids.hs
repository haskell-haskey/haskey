{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures #-}

module Data.BTree.Primitives.Ids where

import Data.BTree.Primitives.Height
import Data.BTree.Primitives.Key
import Data.BTree.Primitives.Value

import Data.Binary (Binary)
import Data.Hashable (Hashable)
import Data.Typeable (Typeable)
import Data.Word
import Numeric (showHex)

--------------------------------------------------------------------------------

-- | Reference to a stored page.
newtype PageId = PageId { fromPageId :: Word64 }
  deriving (Eq, Ord, Binary, Num, Value, Key, Typeable)

-- | Reference to a stored overflow page.
--
-- An overflow id is the combination of the transaction id that
-- generated it, and a counter.
type OverflowId = (TxId, Word32)

-- | Type used to indicate the size of storage pools.
newtype PageCount = PageCount { fromPageCount :: Word64 }
  deriving (Eq, Ord, Binary, Num, Enum, Typeable)

-- | Type used to indicate the size of a single physical page in bytes.
newtype PageSize = PageSize { fromPageSize :: Word32 }
  deriving (Eq, Ord, Show, Binary, Num, Enum, Real, Integral, Typeable)

-- | Reference to a stored 'Node'.
--
-- 'NodeId' has phantom type arguments for the parameters of 'Node' to be able
-- to enforce consistency. In a setting with a single storage pool this 'Id'
-- will essentially be a 'PageId' with just the extra typing. In a multi
-- storage pool setting 'NodeId's will additionally have to be resolved to
-- 'PageId's by the node allocator.
newtype NodeId (height :: Nat) key val = NodeId { fromNodeId :: Word64 }
  deriving (Eq, Ord, Binary, Num)

-- | Convert a 'NodeId' to a 'PageId'
nodeIdToPageId :: NodeId height key val -> PageId
nodeIdToPageId = PageId . fromNodeId

-- | Convert a 'PageId' to a 'NodeId'
pageIdToNodeId :: PageId -> NodeId height key val
pageIdToNodeId = NodeId . fromPageId

-- | Transaction ids that are used as revision numbers.
newtype TxId = TxId { fromTxId :: Word64 }
  deriving (Eq, Ord, Binary, Num, Hashable, Value, Key, Typeable)

instance Show PageId where
    showsPrec _ (PageId n) = showString "0x" . showHex n
instance Show PageCount where
    showsPrec _ (PageCount n) = showString "0x" . showHex n
instance Show (NodeId height key val) where
    showsPrec _ (NodeId n) = showString "0x" . showHex n
instance Show TxId where
    showsPrec _ (TxId n) = showString "0x" . showHex n

--------------------------------------------------------------------------------
