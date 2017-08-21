
module Data.BTree.Primitives.Key where

import Data.ByteString (ByteString)
import Data.Int
import Data.Word
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BS

import Data.BTree.Primitives.Exception
import Data.BTree.Primitives.Value

--------------------------------------------------------------------------------

class (Ord k, Value k) => Key k where
    -- | Given two keys 'a', 'b' such that 'a < b' compute two new keys 'a2',
    -- 'b2' such that 'a <= a2 < b2 <= b'. Obviously this always holds for 'a2
    -- == a' and 'b2 = b' but for 'ByteString's we can potentially find smaller
    -- 'a2' and 'b2'. If 'a' equals 'b', the behaviour is undefined.
    narrow :: k -> k -> (k,k)
    narrow = (,)

instance Key ()
instance Key Bool
instance Key Double
instance Key Float
instance Key Int8
instance Key Int16
instance Key Int32
instance Key Int64
instance Key Integer
instance Key Word8
instance Key Word16
instance Key Word32
instance Key Word64

instance Key ByteString where
    narrow a b =
      case (compare n na, compare n nb) of
        -- So the n+1th byte is the first distinguishing byte.
        (LT,LT) -> (BS.unsafeTake (n+1) a, BS.unsafeTake (n+1) b)
        -- In this case 'a' is a prefix of 'b'. Can't do anything for a, but we
        -- can shorten 'b'.
        (EQ,LT) -> (a, BS.unsafeTake (n+1) b)
        -- Inputs violate the invariant a<b
        _  -> throw $ TreeAlgorithmError "narrow (Binary)" $ concat
              ["Key ByteString: can't narrow ", show a, " and ", show b]
      where
        na = BS.length a
        nb = BS.length b
        -- Length of the longest Common prefix
        n  = length (takeWhile id (BS.zipWith (==) a b))

instance (Key a, Key b) => Key (a, b) where

--------------------------------------------------------------------------------
