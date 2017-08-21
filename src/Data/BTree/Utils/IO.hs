module Data.BTree.Utils.IO where

import Data.ByteString (ByteString, packCStringLen)
import Data.ByteString.Unsafe (unsafeUseAsCString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL

import Foreign (allocaBytes, castPtr, plusPtr)

import qualified FileIO as IO

import System.IO.Error (mkIOError, eofErrorType, ioeSetErrorString)

readByteString :: IO.FHandle -> Int -> IO ByteString
readByteString fd n = allocaBytes n $ \buf -> do
    go 0 buf
    packCStringLen (castPtr buf, fromIntegral n)
  where
    go c buf
        | c == n = return ()
        | otherwise = do
            rc <- IO.read fd buf (fromIntegral (n - c))
            case rc of
                0 -> ioError (ioeSetErrorString (mkIOError eofErrorType "getByteString" Nothing Nothing) "EOF")
                n' -> go (c + fromIntegral n') (buf `plusPtr` fromIntegral n')

writeByteString :: IO.FHandle -> ByteString -> IO ()
writeByteString fd bs = unsafeUseAsCString bs $ \buf -> go 0 buf
  where
    n = BS.length bs
    go c buf
        | c == n = return ()
        | otherwise = do
            n' <- IO.write fd (castPtr buf) (fromIntegral (n - c))
            go (c + fromIntegral n') (buf `plusPtr` fromIntegral n')

writeLazyByteString :: IO.FHandle -> BL.ByteString -> IO ()
writeLazyByteString fh bs = mapM_ (writeByteString fh) (BL.toChunks bs)
