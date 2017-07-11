module Properties.Utils where

import Test.QuickCheck

import Control.Applicative ((<$>))

import Data.Int
import qualified Data.Binary as B

testBinary :: (Eq a, B.Binary a) => a -> Bool
testBinary x = B.decode (B.encode x) == x

newtype PageSize = PageSize Int32 deriving (Show)

instance Arbitrary PageSize where
    arbitrary = PageSize . fromIntegral <$> elements pows
      where pows = ((2 :: Int) ^) <$> ([6..12] :: [Int])
