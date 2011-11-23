package jdbm;

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
                if(t.isAccessible()){
                    Object o = t.newInstance();
                    classesWithConstructors.add(clazz.getName());
                    return;
                }
            }catch(Exception e){}
            throw new IllegalArgumentException(("Class does not have public noarg constructor: "+clazz.getName()));
        }


        public void serialize(DataOutput out, Object obj) throws IOException {
            if(obj == null)
                out.writeUTF(EMPTY_STRING);
            else{
                assertHasPublicNoArgConstructor(obj.getClass());
                out.writeUTF(obj.getClass().getName());
            }
        }

        public Object deserialize(DataInput in) throws IOException, ClassNotFoundException {
            String className = in.readUTF();
            if("".equals(className))
                return null;
            try {
                Class clazz = Utils.class.getClassLoader().loadClass(className);
                return clazz.getConstructor().newInstance();
            } catch (Exception e) {
                throw new Error("An error when creating an instance of: "+className,e);
            }

        }
    };


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



}
