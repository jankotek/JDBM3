package jdbm;

import java.awt.image.ImagingOpException;
import java.io.*;
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
        private final List<FieldInfo> fields = new ArrayList<FieldInfo>();

        ClassInfo(String name, FieldInfo[] fields){
            this.name = name;
            for(FieldInfo f:fields){
                this.fields.add(f);
            }
        }

        public String getName(){
            return name;
        }

        public FieldInfo[] getFields(){
            return (FieldInfo[]) fields.toArray();
        }

        public FieldInfo getField(String name){
            for(FieldInfo field:fields){
                if(field.getName().equals(name))
                    return field;
            }
            return null;
        }

        public int getFieldId(String name){
            for(int i=0;i<fields.size();i++){
                if(fields.get(i).getName().equals(name))
                    return i;
            }
            throw new Error("Field not found: "+name);
        }

        public FieldInfo getField(int serialId){
            return fields.get(serialId);
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


    private List<ClassInfo> registered = new ArrayList<ClassInfo>();



    public void registerClass(Class clazz) throws NotSerializableException, InvalidClassException {
        assertClassSerializable(clazz);

        if(containsClass(clazz))
            return;

        ObjectStreamClass streamClass = ObjectStreamClass.lookup(clazz);

        ObjectStreamField[] streamFields = streamClass.getFields();
        FieldInfo[] fields = new FieldInfo[streamFields.length];
        for(int i=0;i<fields.length;i++){
            ObjectStreamField sf = streamFields[i];
            fields[i] = new FieldInfo(sf.getName(),sf.isPrimitive(),sf.getType());
        }

        ClassInfo i = new ClassInfo(clazz.getName(),fields);
        registered.add(i);
    }

    private void assertClassSerializable(Class clazz) throws NotSerializableException, InvalidClassException {
        if(!Serializable.class.isAssignableFrom(clazz))
            throw new NotSerializableException(clazz.getName());
        try {
            clazz.getDeclaredConstructor();
        } catch (NoSuchMethodException e) {
            throw new InvalidClassException("There is no no-arg constructor at: "+clazz.getName());
        }
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


    private String firstCharCap(String s){
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    public boolean containsClass(Class clazz) {
        for(ClassInfo c:registered){
            if(c.getName().equals(clazz.getName()))
                return true;
        }
        return false;
    }

    public int getClassId(Class clazz){
        for(int i = 0;i<registered.size();i++){
            if(registered.get(i).getName().equals(clazz.getName()))
                return i;
        }
        throw new Error("Class is not registered: "+clazz);
    }

    public void writeObject(DataOutputStream out, Object obj) throws IOException {
        registerClass(obj.getClass());

        //write class header
        int classId = getClassId(obj.getClass());
        LongPacker.packInt(out,classId);
        ClassInfo classInfo = registered.get(classId);

        ObjectStreamClass streamClass = ObjectStreamClass.lookup(obj.getClass());
        LongPacker.packInt(out,streamClass.getFields().length);

        for(ObjectStreamField f:streamClass.getFields()){
            //write field ID
            int fieldId = classInfo.getFieldId(f.getName());
            LongPacker.packInt(out,fieldId);
            //and write value
            Object fieldValue = getFieldValue(f.getName(),obj);
            new Serialization().serialize(out,fieldValue);
        }


    }

    public Object readObject(DataInputStream in) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        //read class header
        int classId = LongPacker.unpackInt(in);
        ClassInfo classInfo = registered.get(classId);
        Class clazz = Class.forName(classInfo.getName());
        assertClassSerializable(clazz);

        Object o = Class.forName(classInfo.getName()).newInstance();

        int fieldCount = LongPacker.unpackInt(in);
        for(int i=0; i<fieldCount; i++){
            int fieldId = LongPacker.unpackInt(in);
            FieldInfo f = classInfo.getField(fieldId);
            Object fieldValue = new Serialization().deserialize(in);
            setFieldValue(f.getName(),o,fieldValue);
        }
        return o;
    }




}
