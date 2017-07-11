
import           Data.BTree.Alloc.Append
import           Data.BTree.Store.Binary
import qualified Data.BTree.Store.File as FS
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
    putStrLn "In-memory:"
    print' test
    putStrLn (renderStyle (style {lineLength=260}) (ppDoc test))

    putStrLn "File:"
    print' =<< testFile "/tmp/db.haskey"
  where
    print' :: Show a => a -> IO ()
    print' = putStrLn . renderStyle (style {lineLength=260}) . ppDoc

--------------------------------------------------------------------------------

testFile :: FilePath -> IO (Maybe (AppendDb FilePath Integer Integer), FS.Files FilePath)
testFile fp = FS.runStore fp $
    createAppendDb fp
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
