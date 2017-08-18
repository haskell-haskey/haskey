{-# LANGUAGE GADTs #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures #-}

module Data.BTree.Primitives.Height where

import Data.Binary (Binary)
import Data.Word (Word8)
import Unsafe.Coerce

--------------------------------------------------------------------------------

data Nat = Z | S Nat

newtype Height (h :: Nat) = Height { fromHeight :: Word8 }
    deriving (Binary, Eq, Ord)

instance Show (Height h) where
    showsPrec p = showsPrec p . fromHeight

zeroHeight :: Height 'Z
zeroHeight = Height 0
{-# INLINE zeroHeight #-}

incrHeight :: Height h -> Height ('S h)
incrHeight = Height . (+1) . fromHeight
{-# INLINE incrHeight #-}

decrHeight :: Height ('S h) -> Height h
decrHeight = Height . (+(-1)) . fromHeight
{-# INLINE decrHeight #-}

--------------------------------------------------------------------------------

data UHeight (height :: Nat) :: * where
    UZero :: UHeight 'Z
    USucc :: Height height -> UHeight ('S height)

viewHeight :: Height height -> UHeight height
viewHeight (Height 0) = unsafeCoerce UZero
viewHeight (Height n) = unsafeCoerce (USucc (Height (n-1)))

--------------------------------------------------------------------------------
