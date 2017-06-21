
import           Data.BTree.Alloc.Append
import           Data.BTree.Store.Debug
import           Data.BTree.Insert
import           Data.BTree.Delete

import           Control.Monad.Identity
import qualified Data.Map as M
import           Prelude
import           Text.PrettyPrint
import           Text.Show.Pretty hiding (Value)

--------------------------------------------------------------------------------

test :: (Maybe (AppendDb String Integer Integer), Files String)
test = runIdentity . flip runStoreT initialStore $
    createAppendDb "Main"
    >>= transact
        (   insertTree 1 0x0
        >=> insertTree 2 0x1
        >=> insertTree 4 0x2
        >=> insertTree 5 0x3
        >=> deleteTree 2
        >=> insertTree 4 0x4
        >=> insertTree 3 0x5
        >=> deleteTree 1
        >=> deleteTree 3
        >=> deleteTree 4
        >=> deleteTree 5
        )
  where
    initialStore :: Files String
    initialStore = M.fromList [ ("Main", M.empty) ]

main :: IO ()
main = do
    putStrLn (renderStyle (style {lineLength=260}) (ppDoc test))

--------------------------------------------------------------------------------
