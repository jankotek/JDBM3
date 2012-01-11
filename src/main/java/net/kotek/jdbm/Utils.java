package net.kotek.jdbm;

import javax.crypto.Cipher;
import java.io.*;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Various utilities used in JDBM
 */
class Utils {

    /**
     * empty string is used as dummy value to represent null values in HashSet and TreeSet
     */
    static final String EMPTY_STRING = "";

    public static Storage storageFab(String fileName) throws IOException {
        Storage ret = null;
        if (fileName.contains("!/"))
            ret = new StorageZip(fileName);
        else
            ret = new StorageDisk(fileName);
        return ret;
    }


    public static byte[] encrypt(Cipher cipherIn, ByteBuffer b) {
        if(cipherIn==null && b.hasArray())
            return b.array();
        byte[] bb = new byte[Storage.BLOCK_SIZE];
        b.rewind();
        b.get(bb,0,Storage.BLOCK_SIZE);
        return encrypt(cipherIn,bb);
    }
    
    public static byte[] encrypt(Cipher cipherIn, byte[] b) {
        if (cipherIn == null)
            return b;

        try {
            return cipherIn.doFinal(b);
        } catch (Exception e) {
            throw new IOError(e);
        }

    }


    /**
     * Compares comparables. Default comparator for most of java types
     */
    static final Comparator COMPARABLE_COMPARATOR = new Comparator<Comparable>() {
        public int compare(Comparable o1, Comparable o2) {
            return o1.compareTo(o2);
        }
    };



    /**
     * Growable long[]
     */
    static final class LongArrayList {
        int size = 0;
        long[] data = new long[16];

        void add(long l) {
            if (data.length == size) {
                //grow
                data = Arrays.copyOf(data, size * 2);
            }
            data[size] = l;
            size++;
        }


        void removeLast() {
            size--;
            data[size] = 0;
        }

        public void clear() {
            if (data.length > 128)
                data = new long[16];
            else{
                for(int i=0;i<size;i++)
                    data[i] = 0L;
            }
                
            size = 0;
        }
    }

    /**
     * Growable int[]
     */
    static final class IntArrayList {
        int size = 0;
        int[] data = new int[32];

        void add(int l) {
            if (data.length == size) {
                //grow
                data = Arrays.copyOf(data, size * 2);
            }
            data[size] = l;
            size++;
        }


        void removeLast() {
            size--;
            data[size] = 0;
        }

        public void clear() {
            if (data.length > 128)
                data = new int[32];
            else{
                for(int i=0;i<size;i++)
                    data[i] = 0;
            }
            size = 0;
        }
    }


    static String formatSpaceUsage(long size) {
        if (size < 1e4)
            return size + "B";
        else if (size < 1e7)
            return "" + Math.round(1D * size / 1024D) + "KB";
        else if (size < 1e10)
            return "" + Math.round(1D * size / 1e6) + "MB";
        else
            return "" + Math.round(1D * size / 1e9) + "GB";
    }


    static boolean allZeros(byte[] b) {
        for (int i = 0; i < b.length; i++) {
            if (b[i] != 0) return false;
        }
        return true;
    }


//    public void write(byte[] data, ) throws IOException {
//        try {
//            s.write(pageNumber,cipherIn.doFinal(data));
//        } catch (IllegalBlockSizeException e) {
//            throw new IOError(e);
//        } catch (BadPaddingException e) {
//            throw new IOError(e);
//        }
//    }
//
//    public boolean read(long pageNumber, byte[] data) throws IOException {
//
//           byte[] buf = new byte[BLOCK_SIZE];
//           if(s.read(pageNumber,buf)){
//               try {
//                    cipherOut.doFinal(buf, 0, BLOCK_SIZE, data);
//               } catch (IllegalBlockSizeException e) {
//                   throw new IOError(e);
//               } catch (BadPaddingException e) {
//                   throw new IOError(e);
//               } catch (ShortBufferException e) {
//                   throw new IOError(e);
//               }
//
//               return true;
//           }else{
//              //page does not exist in underlying store
//              System.arraycopy(RecordFile.CLEAN_DATA,0,data,0,BLOCK_SIZE);
//              return false;
//           }
//
//    }


}
