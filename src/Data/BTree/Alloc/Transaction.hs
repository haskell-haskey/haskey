{-| This module implements mechanisms to work with transactions. -}
module Data.BTree.Alloc.Transaction where

import Data.BTree.Alloc.Class
import Data.BTree.Impure.Structures

{-| A committed or aborted transaction, with a return value of type @a@. -}
data Transaction key val a =
      Commit (Tree key val) a
    | Abort a

{-| Commit the new tree and return a computed value. -}
commit :: AllocM n => a -> Tree key val -> n (Transaction key val a)
commit v t = return $ Commit t v

{-| Commit the new tree, without return a computed value. -}
commit_ :: AllocM n => Tree key val -> n (Transaction key val ())
commit_ = commit ()

{-| Abort the transaction and return a computed value. -}
abort :: AllocM n => a -> n (Transaction key val a)
abort = return . Abort

{-| Abort the transaction, without returning a computed value. -}
abort_ :: AllocM n => n (Transaction key val ())
abort_ = return $ Abort ()
