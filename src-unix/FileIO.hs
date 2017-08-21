-- | Module exporting some low level file primitives for Unix.
--
-- This source file was taken from acid-state-0.14.3, and slightly modified.
module FileIO (
    FHandle
  , openReadWrite
  , write
  , read
  , flush
  , close
  , seek
  , setFileSize
  , getFileSize
  , obtainPrefixLock
  , releasePrefixLock
  , PrefixLock
) where

import Prelude hiding (read)
import qualified Prelude as P

import Control.Applicative ((<$>))
import Control.Exception (SomeException(..), throw, try)
import Control.Monad (void)

import Data.Maybe (listToMaybe)
import Data.Word (Word8, Word64)

import Foreign (Ptr)

import System.Directory (createDirectoryIfMissing, removeFile)
import System.FilePath
import System.IO
import System.Posix (Fd,
                     openFd,
                     fdReadBuf,
                     fdWriteBuf,
                     fdToHandle,
                     fdSeek,
                     setFdSize,
                     fileSynchronise,
                     closeFd,
                     OpenMode(ReadWrite),
                     exclusive, trunc,
                     defaultFileFlags,
                     stdFileMode)
import System.Posix.Process (getProcessID)
import System.Posix.Signals (nullSignal, signalProcess)
import System.Posix.Types (ProcessID)
import qualified System.IO.Error as SE


newtype PrefixLock = PrefixLock FilePath

newtype FHandle = FHandle Fd

-- | Open the specified file in read-write mode.
openReadWrite :: FilePath -> IO FHandle
openReadWrite filename =
    FHandle <$> openFd filename ReadWrite (Just stdFileMode) defaultFileFlags

-- | Write **at most** the specified amount of bytes to a handle.
write :: FHandle -> Ptr Word8 -> Word64 -> IO Word64
write (FHandle fd) data' length' =
    fmap fromIntegral $ fdWriteBuf fd data' $ fromIntegral length'

-- | Read **at most** the specified amount of bytes from a handle.
--
-- Return the amount of bytes actually read, or throw an IO error (including
-- EOF).
read :: FHandle -> Ptr Word8 -> Word64 -> IO Word64
read (FHandle fd) buf len =
    fromIntegral <$> fdReadBuf fd buf (fromIntegral len)

-- | Synchronize the file contents to disk.
flush :: FHandle -> IO ()
flush (FHandle fd) = fileSynchronise fd

-- | Seek to an absolute position.
seek :: FHandle -> Word64 -> IO ()
seek (FHandle fd) offset = void $ fdSeek fd AbsoluteSeek (fromIntegral offset)

-- | Set the filesize to a certain length.
setFileSize :: FHandle -> Word64 -> IO ()
setFileSize (FHandle fd) size = setFdSize fd (fromIntegral size)

-- | Get the filesize. This **edits the file pointer**.
getFileSize :: FHandle -> IO Word64
getFileSize (FHandle fd) = fromIntegral <$> fdSeek fd SeekFromEnd 0

-- | Close a file.
close :: FHandle -> IO ()
close (FHandle fd) = closeFd fd

-- Unix needs to use a special open call to open files for exclusive writing
--openExclusively :: FilePath -> IO Handle
--openExclusively fp =
--    fdToHandle =<< openFd fp ReadWrite (Just 0o600) flags
--    where flags = defaultFileFlags {exclusive = True, trunc = True}




obtainPrefixLock :: FilePath -> IO PrefixLock
obtainPrefixLock prefix = checkLock fp >> takeLock fp
    where fp = prefix ++ ".lock"

-- |Read the lock and break it if the process is dead.
checkLock :: FilePath -> IO ()
checkLock fp = readLock fp >>= maybeBreakLock fp

-- |Read the lock and return the process id if possible.
readLock :: FilePath -> IO (Maybe ProcessID)
readLock fp = do
    pid <- try (readFile fp)
    return $ either (checkReadFileError fp)
                    (fmap (fromInteger . P.read) . listToMaybe . lines)
                    pid

-- |Is this a permission error?  If so we don't have permission to
-- remove the lock file, abort.
checkReadFileError :: String -> IOError -> Maybe ProcessID
checkReadFileError fp e | SE.isPermissionError e = throw (userError ("Could not read lock file: " ++ show fp))
                        | SE.isDoesNotExistError e = Nothing
                        | otherwise = throw e

maybeBreakLock :: FilePath -> Maybe ProcessID -> IO ()
maybeBreakLock fp Nothing =
    -- The lock file exists, but there's no PID in it.  At this point,
    -- we will break the lock, because the other process either died
    -- or will give up when it failed to read its pid back from this
    -- file.
    breakLock fp
maybeBreakLock fp (Just pid) = do
  -- The lock file exists and there is a PID in it.  We can break the
  -- lock if that process has died.
  -- getProcessStatus only works on the children of the calling process.
  -- exists <- try (getProcessStatus False True pid) >>= either checkException (return . isJust)
  exists <- doesProcessExist pid
  if exists
    then throw (lockedBy fp pid)
    else breakLock fp

doesProcessExist :: ProcessID -> IO Bool
doesProcessExist pid = do
    -- Implementation 1
    -- doesDirectoryExist ("/proc/" ++ show pid)
    -- Implementation 2
    v <- try (signalProcess nullSignal pid)
    return $ either checkException (const True) v
  where checkException e | SE.isDoesNotExistError e = False
                         | otherwise = throw e

-- |We have determined the locking process is gone, try to remove the
-- lock.
breakLock :: FilePath -> IO ()
breakLock fp = try (removeFile fp) >>= either checkBreakError (const (return ()))

-- |An exception when we tried to break a lock, if it says the lock
-- file has already disappeared we are still good to go.
checkBreakError :: IOError -> IO ()
checkBreakError e | SE.isDoesNotExistError e = return ()
                  | otherwise = throw e

-- |Try to create lock by opening the file with the O_EXCL flag and
-- writing our PID into it.  Verify by reading the pid back out and
-- matching, maybe some other process slipped in before we were done
-- and broke our lock.
takeLock :: FilePath -> IO PrefixLock
takeLock fp = do
  createDirectoryIfMissing True (takeDirectory fp)
  h <- openFd fp ReadWrite (Just 0o600) (defaultFileFlags {exclusive = True, trunc = True}) >>= fdToHandle
  pid <- getProcessID
  hPrint h pid >> hClose h
  -- Read back our own lock and make sure its still ours
  readLock fp >>= maybe (throw (cantLock fp pid))
                        (\ pid' -> if pid /= pid'
                                   then throw (stolenLock fp pid pid')
                                   else return (PrefixLock fp))

-- |An exception saying the data is locked by another process.
lockedBy :: (Show a) => FilePath -> a -> SomeException
lockedBy fp pid = SomeException (SE.mkIOError SE.alreadyInUseErrorType ("Locked by " ++ show pid) Nothing (Just fp))

-- |An exception saying we don't have permission to create lock.
cantLock :: FilePath -> ProcessID -> SomeException
cantLock fp pid = SomeException (SE.mkIOError SE.alreadyInUseErrorType ("Process " ++ show pid ++ " could not create a lock") Nothing (Just fp))

-- |An exception saying another process broke our lock before we
-- finished creating it.
stolenLock :: FilePath -> ProcessID -> ProcessID -> SomeException
stolenLock fp pid pid' = SomeException (SE.mkIOError SE.alreadyInUseErrorType ("Process " ++ show pid ++ "'s lock was stolen by process " ++ show pid') Nothing (Just fp))

-- |Relinquish the lock by removing it and then verifying the removal.
releasePrefixLock :: PrefixLock -> IO ()
releasePrefixLock (PrefixLock fp) =
    dropLock >>= either checkDrop return
    where
      dropLock = try (removeFile fp)
      checkDrop e | SE.isDoesNotExistError e = return ()
                  | otherwise = throw e
