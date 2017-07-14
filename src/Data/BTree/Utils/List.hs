module Data.BTree.Utils.List where

import Data.Maybe (listToMaybe)

safeLast :: [a] -> Maybe a
safeLast = listToMaybe . reverse
