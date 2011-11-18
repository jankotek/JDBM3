package jdbm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Constructor;
import java.util.*;

/**
 * Various utilities used in JDBM
 */
class Utils {

    private static final Set<String> classesWithConstructors = new HashSet<String>();
    /** empty string is used as dummy value to represent null values in HashSet and TreeSet */
    static final String EMPTY_STRING = "";

    /**
     * Throws IllegalArgumentExcpetion if given class does not have public no-argument constructor
     * @param clazz
     */
    static void assertHasPublicNoArgConstructor(Class clazz){

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


    /**
     * Create an instance of object with given class name
     * @param className
     * @return
     */
    static Object loadInstance(String className) {
        if("".equals(className))
            return null;
        try {
            Class clazz = Utils.class.getClassLoader().loadClass(className);
            return clazz.getConstructor().newInstance();
        } catch (ClassNotFoundException e) {
            throw new Error("Could not load class referenced from JDBM store: "+className);
        } catch (Exception e) {
            throw new Error("An error when creating an instance of: "+className,e);
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

    /*** Compares comparables. Default comparator for most of java types*/
    static final Comparator COMPARABLE_COMPARATOR =  new Comparator<Comparable>(){
	public int compare(Comparable o1, Comparable o2) {
            return o1.compareTo(o2);
        }
    };
}
