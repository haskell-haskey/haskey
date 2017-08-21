-- | This module contains structures relating the the setup of a pure B+-tree.
module Data.BTree.Pure.Setup (
  -- * Setup
  TreeSetup(..)

  -- * Predefined setups
, twoThreeSetup
, setupWithMinimumDegreeOf
) where

-- | Setup of a pure B+-tree.
data TreeSetup = TreeSetup {
    minFanout :: Int
  , maxFanout :: Int
  , minIdxKeys :: Int
  , maxIdxKeys :: Int
  , minLeafItems :: Int
  , maxLeafItems :: Int
  } deriving (Show)

-- | Setup of a 2-3 tree.
twoThreeSetup :: TreeSetup
twoThreeSetup = TreeSetup {
    minFanout = minFanout'
  , maxFanout = maxFanout'
  , minIdxKeys = minFanout' - 1
  , maxIdxKeys = maxFanout' - 1
  , minLeafItems = minFanout'
  , maxLeafItems = 2*minFanout' - 1
  }
  where
    minFanout' = 2
    maxFanout' = 2*minFanout' - 1

-- | Setup of a B+-tree with a certain minimum degree, as defined in CLRS.
--
-- To get for example a 2-3-4 tree, use
--
-- >>> setupWithMinimumDegreeOf 2
--
setupWithMinimumDegreeOf :: Int -> TreeSetup
setupWithMinimumDegreeOf deg = TreeSetup {
    minFanout = minFanout'
  , maxFanout = maxFanout'
  , minIdxKeys = minFanout' - 1
  , maxIdxKeys = maxFanout' - 1
  , minLeafItems = minFanout' - 1
  , maxLeafItems = maxFanout' - 1
  }
  where
    minFanout' = deg
    maxFanout' = 2*deg
