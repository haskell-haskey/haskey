{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
-- | Basic structures of an impure B+-tree.
module Data.BTree.Impure.Structures (
  -- * Structures
  Tree(..)
, Node(..)
, LeafItems
, LeafValue(..)

  -- * Binary encoding
, putLeafNode
, getLeafNode
, putIndexNode
, getIndexNode

  -- * Casting
, castNode
, castNode'
, castValue
) where

import Control.Applicative ((<$>), (<*>))
import Control.Monad (replicateM)

import Data.Binary (Binary(..), Put, Get)
import Data.Bits ((.|.), shiftL, shiftR)
import Data.Map (Map)
import Data.Proxy (Proxy(..))
import Data.Typeable (Typeable, typeRep, cast)
import Data.Word (Word8, Word32)
import qualified Data.Map as M

import Numeric (showHex)

import Unsafe.Coerce

import Data.BTree.Primitives

--------------------------------------------------------------------------------

-- | A B+-tree.
--
-- This is a simple wrapper around a root 'Node'. The type-level height is
-- existentially quantified, but a term-level witness is stores.
data Tree key val where
    Tree :: { -- | A term-level witness for the type-level height index.
              treeHeight :: Height height
            , -- | An empty tree is represented by 'Nothing'. Otherwise it's
              --   'Just' a 'NodeId' pointer the root.
              treeRootId :: Maybe (NodeId height key val)
            } -> Tree key val
    deriving (Typeable)

data LeafValue v = RawValue v | OverflowValue OverflowId
                 deriving (Eq, Show)

instance Binary v => Binary (LeafValue v) where
    put (RawValue v) = put (0x00 :: Word8) >> put v
    put (OverflowValue v) = put (0x01 :: Word8) >> put v

    get = (get :: Get Word8) >>= \case
        0x00 -> RawValue <$> get
        0x01 -> OverflowValue <$> get
        t -> fail $ "unknown leaf value: " ++ showHex t ""

type LeafItems k v = Map k (LeafValue v)

-- | A node in a B+-tree.
--
--  Nodes are parameterized over the key and value types and are additionally
--  indexed by their height. All paths from the root to the leaves have the same
--  length. The height is the number of edges from the root to the leaves,
--  i.e. leaves are at height zero and index nodes increase the height.
--
--  Sub-trees are represented by a 'NodeId' that are used to resolve the actual
--  storage location of the sub-tree node.
data Node height key val where
    Idx  :: { idxChildren      ::  Index key (NodeId height key val)
            } -> Node ('S height) key val
    Leaf :: { leafItems        ::  LeafItems key val
            } -> Node 'Z key val
    deriving (Typeable)

instance (Eq key, Eq val) => Eq (Node height key val) where
    Leaf x == Leaf y = x == y
    Idx x  == Idx y  = x == y

deriving instance (Show key, Show val) => Show (Node height key val)
deriving instance (Show key, Show val) => Show (Tree key val)

instance (Value k, Value v) => Value (Tree k v) where

--------------------------------------------------------------------------------

instance Binary (Tree key val) where
    put (Tree height rootId) = put height >> put rootId
    get = Tree <$> get <*> get

-- | Encode a 'Leaf' 'Node'.
putLeafNode :: (Binary key, Binary val) => Node 'Z key val -> Put
putLeafNode (Leaf items) = do
    encodeSize $ fromIntegral (M.size items)
    mapM_ put $ M.toList items
  where
    encodeSize :: Word32 -> Put
    encodeSize s = put msb1 >> put msb2 >> put msb3
      where
        msb1 = fromIntegral $ s `shiftR` 16 :: Word8
        msb2 = fromIntegral $ s `shiftR`  8 :: Word8
        msb3 = fromIntegral   s             :: Word8

-- | Decode a 'Leaf' 'Node'.
getLeafNode :: (Ord key, Binary key, Binary val) => Height 'Z -> Get (Node 'Z key val)
getLeafNode _ = do
    v <- decodeSize <$> get
    l <- replicateM (fromIntegral v) get
    return $ Leaf (M.fromList l)
  where
    decodeSize :: (Word8, Word8, Word8) -> Word32
    decodeSize (msb1, msb2, msb3) = msb1' .|. msb2' .|. msb3'
      where
        msb1' = (fromIntegral msb1 :: Word32) `shiftL` 16
        msb2' = (fromIntegral msb2 :: Word32) `shiftL`  8
        msb3' =  fromIntegral msb3 :: Word32

-- | Encode an 'Idx' 'Node'.
putIndexNode :: (Binary key, Binary val) => Node ('S n) key val -> Put
putIndexNode (Idx idx) = put idx

-- | Decode an 'Idx' 'Node'.
getIndexNode :: (Binary key, Binary val) => Height ('S n) -> Get (Node ('S n) key val)
getIndexNode _ = Idx <$> get

--------------------------------------------------------------------------------

-- | Cast a node to a different type.
--
-- Essentially this is just a drop-in replacement for 'Data.Typeable.cast'.
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

-- | Cast a node to one of the available types.
castNode' :: forall n h k v.
          (Typeable k, Typeable v)
    => Height h         -- ^ Term-level witness for the source height
    -> n h k v          -- ^ Node to cast.
    -> Either (n 'Z k v) (n ('S h) k v)
castNode' h n
    | Just v <- castNode h zeroHeight n = Left v
    | otherwise                         = Right (unsafeCoerce n)

--------------------------------------------------------------------------------

-- | Cast a value to a different type.
--
-- Essentially this is just a drop-in replacement for
-- 'Data.Typeable.cast'.
castValue :: (Typeable v1, Typeable v2) => v1 -> Maybe v2
castValue = cast
