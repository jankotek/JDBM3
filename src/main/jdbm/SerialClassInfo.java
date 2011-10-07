package jdbm;

import java.io.ObjectStreamClass;
import java.io.ObjectStreamField;
import java.util.*;
import java.lang.reflect.*;

/**
 * This class stores information about serialized classes and fields.
 */
public class SerialClassInfo {


    /**
     * Stores info about single class stored in JDBM.
     * Roughly corresponds to 'java.io.ObjectStreamClass'
     */
    public static class ClassInfo{

        private final String name;
        private final Class mappedClass;
        private final FieldInfo[] fields;

        ClassInfo(String name, FieldInfo[] fields) throws ClassNotFoundException {
            this.name = name;
            this.mappedClass = getClass().getClassLoader().loadClass(name);
            this.fields = fields;
        }

        public String getName(){
            return name;
        }

        public Class getMappedClass(){
            return mappedClass;
        }

        public FieldInfo[] getFields(){
            return Arrays.copyOf(fields,fields.length);
        }

        public FieldInfo getField(String name){
            for(FieldInfo field:fields){
                if(field.getName().equals(name))
                    return field;
            }
            return null;
        }

        public FieldInfo getField(int serialId){
            return fields[serialId];
        }

    }

    /**
     * Stores info about single field stored in JDBM.
     * Roughly corresponds to 'java.io.ObjectFieldClass'
     */
    public static class FieldInfo{

        private final String name;
        private final boolean primitive;
        private final Class type;

        public FieldInfo(String name, boolean primitive, Class type) {
            this.name = name;
            this.primitive = primitive;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public boolean isPrimitive() {
            return primitive;
        }

        public Class getType() {
            return type;
        }

    }


    private Map<String,ClassInfo> registered = new LinkedHashMap<String,ClassInfo>();


    public void registerClass(Class clazz) throws ClassNotFoundException {
        if(registered.containsKey(clazz.getCanonicalName()))
            return;

        ObjectStreamClass streamClass = ObjectStreamClass.lookup(clazz);
        ObjectStreamField[] streamFields = streamClass.getFields();
        FieldInfo[] fields = new FieldInfo[streamFields.length];
        for(int i=0;i<fields.length;i++){
            ObjectStreamField sf = streamFields[i];
            fields[i] = new FieldInfo(sf.getName(),sf.isPrimitive(),sf.getType());
        }

        ClassInfo i = new ClassInfo(clazz.getCanonicalName(),fields);
        registered.put(i.getName(),i);
    }

    public Object getFieldValue(String fieldName, Object object){
        Class clazz = object.getClass();
        //iterate over class hierarchy, until root class
        while(clazz != Object.class){
            //check if there is getMethod
            try{
                Method m = clazz.getDeclaredMethod("get" + firstCharCap(fieldName));
                if(m!=null)
                    return m.invoke(object);
            }catch(Exception e){
            //    e.printStackTrace();
            }
            //no get method, access field directly
            try{
                Field f = clazz.getDeclaredField(fieldName);
                if(!f.isAccessible())
                    f.setAccessible(true);  // security manager may not be happy about this
                return f.get(object);
            }catch(Exception e){
            //    e.printStackTrace();
            }
            //move to superclass
            clazz = clazz.getSuperclass();
        }
        throw new NoSuchFieldError(object.getClass()+"."+fieldName);
    }

    public void setFieldValue(String fieldName, Object object, Object value) {
        Class clazz = object.getClass();
        //iterate over class hierarchy, until root class
        while(clazz != Object.class){
            //check if there is getMethod
            try{
                Method m = clazz.getMethod("set" + firstCharCap(fieldName),value.getClass());
                if(m!=null){
                    m.invoke(object,value);
                    return;
                }
            }catch(Exception e){
            //    e.printStackTrace();
            }
            //no get method, access field directly
            try{
                Field f = clazz.getDeclaredField(fieldName);
                if(!f.isAccessible())
                    f.setAccessible(true);  // security manager may not be happy about this
                f.set(object,value);
                return;
            }catch(Exception e){
            //    e.printStackTrace();
            }
            //move to superclass
            clazz = clazz.getSuperclass();
        }
        throw new NoSuchFieldError(object.getClass()+"."+fieldName);

    }


    static String firstCharCap(String s){
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }
}
