{-# LANGUAGE DataKinds #-}
{-# LANGUAGE KindSignatures #-}

module Data.BTree.Primitives.Height where

--------------------------------------------------------------------------------

data Nat = Z | S Nat

newtype Height (h :: Nat) = Height { fromHeight :: Int }
    deriving (Eq, Ord)

instance Show (Height h) where
    showsPrec p = showsPrec p . fromHeight

zeroHeight :: Height 'Z
zeroHeight = Height 0

incrHeight :: Height h -> Height ('S h)
incrHeight = Height . (+1) . fromHeight

decrHeight :: Height ('S h) -> Height h
decrHeight = Height . (+(-1)) . fromHeight

--------------------------------------------------------------------------------
