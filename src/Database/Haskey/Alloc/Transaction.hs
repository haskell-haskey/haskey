-- | This module implements mechanisms to work with transactions.
module Database.Haskey.Alloc.Transaction where

import Data.BTree.Alloc.Class

-- | A committed or aborted transaction, with a return value of type @a@.
data Transaction r a =
      Commit r a
    | Abort a

-- | Commit the new tree and return a computed value.
commit :: AllocM n => a -> r -> n (Transaction r a)
commit v t = return $ Commit t v

-- | Commit the new tree, without return a computed value.
commit_ :: AllocM n => r -> n (Transaction r ())
commit_ = commit ()

-- | Abort the transaction and return a computed value.
abort :: AllocM n => a -> n (Transaction r a)
abort = return . Abort

-- | Abort the transaction, without returning a computed value.
abort_ :: AllocM n => n (Transaction r ())
abort_ = return $ Abort ()
