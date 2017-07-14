module Properties.Utils where

import qualified Data.Binary as B

testBinary :: (Eq a, B.Binary a) => a -> Bool
testBinary x = B.decode (B.encode x) == x
