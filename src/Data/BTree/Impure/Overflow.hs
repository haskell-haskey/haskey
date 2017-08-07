-- | Functions related to overflow pages.
module Data.BTree.Impure.Overflow where

import Prelude hiding (max)

import Data.Binary (encode)
import Data.Map (Map)
import qualified Data.ByteString.Lazy as BL

import Data.BTree.Alloc.Class
import Data.BTree.Impure.Structures
import Data.BTree.Primitives

toLeafValue :: (AllocM m, Value v)
            => v
            -> m (LeafValue v)
toLeafValue v = do
    max <- maxValueSize
    if BL.length (encode v) <= fromIntegral max
        then return $ RawValue v
        else OverflowValue <$> allocOverflow v

fromLeafValue :: (AllocReaderM m, Value v)
              => LeafValue v
              -> m v
fromLeafValue (RawValue v) = return v
fromLeafValue (OverflowValue (tx, c)) = readOverflow tx c


toLeafItems :: (AllocM m, Value v) => Map k v -> m (LeafItems k v)
toLeafItems = mapM toLeafValue


fromLeafItems :: (AllocReaderM m, Value v) => LeafItems k v -> m (Map k v)
fromLeafItems = mapM fromLeafValue
