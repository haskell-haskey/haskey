{-# LANGUAGE FlexibleContexts #-}
-- | Data structures and functions related to handling overflow pages.
module Data.BTree.Alloc.Concurrent.Overflow where

import Control.Applicative ((<$>))
import Control.Monad.State

import Data.Bits (shiftR)
import Data.Word (Word8)

import Numeric (showHex)

import System.FilePath ((</>), (<.>))

import Data.BTree.Alloc.Concurrent.Environment
import Data.BTree.Primitives

getNewOverflowId :: (Functor m, MonadState (WriterEnv hnd) m)
                 => m OverflowId
getNewOverflowId = do
    tx <- writerTxId <$> get
    c  <- writerOverflowCounter <$> get
    modify' $ \e -> e { writerOverflowCounter = 1 + writerOverflowCounter e }
    return (tx, c)

getOverflowHandle :: FilePath -> OverflowId -> FilePath
getOverflowHandle root (TxId tx, c) =
    root </> lsb1 </> lsb2 </> showHex' tx <.> showHex' c <.> "overflow"
  where
    lsb1 = showHex' (fromIntegral tx :: Word8)
    lsb2 = showHex' (fromIntegral (tx `shiftR` 8) :: Word8)

    showHex' :: (Integral a, Show a) => a -> String
    showHex' = flip showHex ""
