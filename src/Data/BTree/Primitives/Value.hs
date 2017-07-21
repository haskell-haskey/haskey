{-# LANGUAGE ScopedTypeVariables #-}

module Data.BTree.Primitives.Value where

import Control.Applicative ((<$>),(<*>))
import Data.Binary         (Binary)
import Data.ByteString     (ByteString)
import Data.Int
import Data.Proxy          (Proxy (..))
import Data.Typeable
import Data.Word

--------------------------------------------------------------------------------

class (Binary v, Show v, Typeable v) => Value v where
    -- | 'Just' with the size in bytes if 'v' is a fixed sized value, 'Nothing'
    --   if 'v' is variable sized.
    fixedSize :: Proxy v -> Maybe Int
    fixedSize _ = Nothing

instance Value ()     where fixedSize _ = Just 0
instance Value Bool   where fixedSize _ = Just 1
instance Value Char   where fixedSize _ = Just 4
instance Value Double where fixedSize _ = Just 8
instance Value Float  where fixedSize _ = Just 4
instance Value Int8   where fixedSize _ = Just 1
instance Value Int16  where fixedSize _ = Just 2
instance Value Int32  where fixedSize _ = Just 4
instance Value Int64  where fixedSize _ = Just 8
instance Value Word8  where fixedSize _ = Just 1
instance Value Word16 where fixedSize _ = Just 2
instance Value Word32 where fixedSize _ = Just 4
instance Value Word64 where fixedSize _ = Just 8

instance Value ByteString
instance Value Integer

instance (Value k1, Value k2) => Value (k1,k2) where
    fixedSize _ =
        (+) <$> fixedSize (Proxy :: Proxy k1)
            <*> fixedSize (Proxy :: Proxy k2)

instance Value v => Value [v] where
    fixedSize = const Nothing
--------------------------------------------------------------------------------
