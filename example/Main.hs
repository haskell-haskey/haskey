module Main where

import Control.Applicative ((<$>))
import Control.Concurrent.Async (async, wait)
import Control.Monad (void, unless, replicateM)

import Data.BTree.Impure (toList, insertTree)
import Data.ByteString (ByteString)
import Data.Int (Int32)
import Data.Text.Encoding (encodeUtf8)

import Database.Haskey.Alloc.Concurrent (ConcurrentDb,
                                         ConcurrentHandles,
                                         concurrentHandles,
                                         openConcurrentDb,
                                         createConcurrentDb,
                                         transact,
                                         transactReadOnly,
                                         abort,
                                         commit)
import Database.Haskey.Store.File (FileStoreT, Files, newFileStore,
                                   runFileStoreT, defFileStoreConfig)

import Text.Numeral (defaultInflection)
import qualified Text.Numeral.Language.ENG as EN

main :: IO ()
main = do
    store <- newFileStore
    db    <- openOrCreate store

    writers <- mapM (async . writer store db) [1..100]
    readers <- replicateM 100 $ async (reader store db)
    mapM_ wait writers
    mapM_ wait readers
    putStrLn "Done"


writer :: Files FilePath
       -> ConcurrentDb Int32 ByteString
       -> Int32
       -> IO ()
writer store db i = do
    s <- runDatabase store $ transact tx db
    unless s $ putStrLn ("Could not encode " ++ show i)
  where
    value = encodeUtf8 <$> EN.us_cardinal defaultInflection i

    tx tree
        | Just bs <- value = insertTree i bs tree >>= commit True
        | otherwise        = abort False

reader :: Files FilePath
       -> ConcurrentDb Int32 ByteString
       -> IO ()
reader files db = void $ replicateM 100 $ runDatabase files $
    transactReadOnly toList db

openOrCreate :: Files FilePath
             -> IO (ConcurrentDb Int32 ByteString)
openOrCreate store = runDatabase store $ do
    maybeDb <- openConcurrentDb handles
    case maybeDb of
        Nothing -> createConcurrentDb handles
        Just db -> return db

runDatabase :: Files FilePath
            -> FileStoreT FilePath m a
            -> m a
runDatabase files action = runFileStoreT action defFileStoreConfig files

handles :: ConcurrentHandles
handles = concurrentHandles "example-database.haskey"
