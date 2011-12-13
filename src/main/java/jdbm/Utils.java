package jdbm;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;
import java.io.*;
import java.lang.reflect.Constructor;
import java.util.*;

/**
 * Various utilities used in JDBM
 */
class Utils {

    /** empty string is used as dummy value to represent null values in HashSet and TreeSet */
    static final String EMPTY_STRING = "";


    static final Serializer CONSTRUCTOR_SERIALIZER = new Serializer() {

        private final Set<String> classesWithConstructors = new HashSet<String>();

        /**
         * Throws IllegalArgumentExcpetion if given class does not have public no-argument constructor
         */
        private void assertHasPublicNoArgConstructor(Class clazz){

            if(classesWithConstructors.contains(clazz.toString()))
                return;
            try{
                Constructor t = clazz.getConstructor();
                Object o = t.newInstance();
                classesWithConstructors.add(clazz.getName());
                return;
            }catch(Throwable e){
                //e.printStackTrace();
            }
            throw new IllegalArgumentException(("Class does not have public noarg constructor: "+clazz.getName()));
        }


        public void serialize(DataOutput out, Object obj) throws IOException {
            if(obj == null)
                out.writeUTF(EMPTY_STRING);
            else if (obj == java.util.Collections.reverseOrder()){
                out.writeUTF("!!REVERSED!!");
            }else{
                assertHasPublicNoArgConstructor(obj.getClass());
                out.writeUTF(obj.getClass().getName());
            }
        }

        public Object deserialize(DataInput in) throws IOException, ClassNotFoundException {
            String className = in.readUTF();
            if("".equals(className))
                return null;
            if("!!REVERSED!!".equals(className))
                return java.util.Collections.reverseOrder();
            try {
                Class clazz = Utils.class.getClassLoader().loadClass(className);
                return clazz.getConstructor().newInstance();
            } catch (Exception e) {
                throw new Error("An error when creating an instance of: "+className,e);
            }

        }
    };

    public static Storage storageFab(String fileName) throws IOException {
        Storage ret = null;
        if(fileName.contains("!/"))
            ret =  new StorageZip(fileName);
        else
            ret = new StorageDisk(fileName);
        return ret;
    }

    public static byte[] encrypt(Cipher cipherIn, byte[] b) {
        if(cipherIn == null)
            return b;

        try {
            return cipherIn.doFinal(b);
        } catch (Exception e) {
            throw new IOError(e);
        }

    }


    static class OpenByteArrayInputStream extends ByteArrayInputStream {

            public OpenByteArrayInputStream(byte[] buf) {
                    super(buf);
            }

            public byte[] getBuf(){
                    return buf;
            }


            public void reset(byte[] buf, int count){
                    this.buf = buf;
                    this.count = count;
                    this.pos = 0;
                    this.mark = 0;
            }

    }

    static class OpenByteArrayOutputStream extends ByteArrayOutputStream {


            public OpenByteArrayOutputStream(byte[] buf) {
                    this.buf = buf;
            }


            public byte[] getBuf(){
                    return buf;
            }


            public void reset(byte[] buf){
                    this.buf = buf;
                    this.count = 0;
            }

    }

    static final class SerializerOutput extends DataOutputStream {

        public SerializerOutput(OutputStream out) {
            super(out);
        }

        /**
         * Reset counter inside DataOutputStream.
         * Workaround method if SerializerOutput instance is reused
         */
         public void __resetWrittenCounter(){
            written = 0;
         }
    }

    /*** Compares comparables. Default comparator for most of java types*/
    static final Comparator COMPARABLE_COMPARATOR =  new Comparator<Comparable>(){
	public int compare(Comparable o1, Comparable o2) {
            return o1.compareTo(o2);
        }
    };

/**
 * Growable long[]
 */
static final  class LongArrayList {
    int size = 0;
    long[] data = new long[32];

    void add(long l){
        if(data.length==size){
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
        if(data.length>32*8)
            data = new long[32];
        else
            Arrays.fill(data,0L);
        size = 0;
    }
}

/**
 * Growable int[]
 */
static final  class IntArrayList {
    int size = 0;
    int[] data = new int[32];

    void add(int l){
        if(data.length==size){
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
        if(data.length>32*8)
            data = new int[32];
        else
            Arrays.fill(data,0);
        size = 0;
    }
}


    static String formatSpaceUsage(long size){
        if(size<1e4)
            return size+"B";
        else if(size<1e7)
            return ""+Math.round(1D*size/1024D)+"KB";
        else if(size<1e10)
            return ""+Math.round(1D*size/1e6)+"MB";
        else
            return ""+Math.round(1D*size/1e9)+"GB";
    }



    static boolean allZeros(byte[] b){
        for(int i = 0;i<b.length;i++){
            if(b[i]!=0)return false;
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
