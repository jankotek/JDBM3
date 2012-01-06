package net.kotek.jdbm;

import java.io.*;
import java.util.*;
import java.lang.reflect.*;

import net.kotek.jdbm.Serialization.FastArrayList;

/**
 * This class stores information about serialized classes and fields.
 */
abstract class SerialClassInfo {

    static final Serializer<ArrayList<ClassInfo>> serializer = new Serializer<ArrayList<ClassInfo>>() {

        public void serialize(DataOutput out, ArrayList<ClassInfo> obj) throws IOException {
            LongPacker.packInt(out, obj.size());
            for (ClassInfo ci : obj) {
                out.writeUTF(ci.getName());
                LongPacker.packInt(out, ci.fields.size());
                for (FieldInfo fi : ci.fields) {
                    out.writeUTF(fi.getName());
                    out.writeBoolean(fi.isPrimitive());
                    out.writeUTF(fi.getType());
                }
            }

        }

        public ArrayList<ClassInfo> deserialize(DataInput in) throws IOException, ClassNotFoundException {
            int size = LongPacker.unpackInt(in);
            ArrayList<ClassInfo> ret = new ArrayList<ClassInfo>(size);

            for (int i = 0; i < size; i++) {
                String className = in.readUTF();
                int fieldsNum = LongPacker.unpackInt(in);
                FieldInfo[] fields = new FieldInfo[fieldsNum];
                for (int j = 0; j < fieldsNum; j++) {
                    fields[j] = new FieldInfo(in.readUTF(), in.readBoolean(), in.readUTF());
                }
                ret.add(new ClassInfo(className, fields));
            }
            return ret;
        }
    };

    private long serialClassInfoRecid;

    public SerialClassInfo(ArrayList<ClassInfo> registered) {
        this.registered = registered;
    }

    public SerialClassInfo(DBAbstract db, long serialClassInfoRecid) throws IOException {
        this.db = db;
        if (db != null) {
            this.serialClassInfoRecid = serialClassInfoRecid;
            this.registered = db.fetch(serialClassInfoRecid, serializer);
        } else {
            //db can be null for unit testing
            this.registered = new ArrayList<ClassInfo>();
        }
    }

    /**
     * Stores info about single class stored in JDBM.
     * Roughly corresponds to 'java.io.ObjectStreamClass'
     */
    static class ClassInfo {

        private final String name;
        private final List<FieldInfo> fields = new ArrayList<FieldInfo>();

        ClassInfo(String name, FieldInfo[] fields) {
            this.name = name;
            for (FieldInfo f : fields) {
                this.fields.add(f);
            }
        }

        public String getName() {
            return name;
        }

        public FieldInfo[] getFields() {
            return (FieldInfo[]) fields.toArray();
        }

        public FieldInfo getField(String name) {
            for (FieldInfo field : fields) {
                if (field.getName().equals(name))
                    return field;
            }
            return null;
        }

        public int getFieldId(String name) {
            for (int i = 0; i < fields.size(); i++) {
                if (fields.get(i).getName().equals(name))
                    return i;
            }
            return -1;
        }

        public FieldInfo getField(int serialId) {
            return fields.get(serialId);
        }

        public int addFieldInfo(FieldInfo field) {
            fields.add(field);
            return fields.size() - 1;
        }

    }

    /**
     * Stores info about single field stored in JDBM.
     * Roughly corresponds to 'java.io.ObjectFieldClass'
     */
    static class FieldInfo {

        private final String name;
        private final boolean primitive;
        private final String type;

        public FieldInfo(String name, boolean primitive, String type) {
            this.name = name;
            this.primitive = primitive;
            this.type = type;
        }

        public FieldInfo(ObjectStreamField sf) {
            this(sf.getName(), sf.isPrimitive(), sf.getType().getName());
        }

        public String getName() {
            return name;
        }

        public boolean isPrimitive() {
            return primitive;
        }

        public String getType() {
            return type;
        }

    }


    private ArrayList<ClassInfo> registered;

    private DBAbstract db = null;


    public void registerClass(Class clazz) throws IOException {
        assertClassSerializable(clazz);

        if (containsClass(clazz))
            return;

        ObjectStreamClass streamClass = ObjectStreamClass.lookup(clazz);

        ObjectStreamField[] streamFields = streamClass.getFields();
        FieldInfo[] fields = new FieldInfo[streamFields.length];
        for (int i = 0; i < fields.length; i++) {
            ObjectStreamField sf = streamFields[i];
            fields[i] = new FieldInfo(sf);
        }

        ClassInfo i = new ClassInfo(clazz.getName(), fields);
        registered.add(i);

        if (db != null)
            db.update(serialClassInfoRecid, registered, serializer);

    }

    private void assertClassSerializable(Class clazz) throws NotSerializableException, InvalidClassException {
        if (!Serializable.class.isAssignableFrom(clazz))
            throw new NotSerializableException(clazz.getName());
    }


    public Object getFieldValue(String fieldName, Object object) {
        Class clazz = object.getClass();
        //iterate over class hierarchy, until root class
        while (clazz != Object.class) {
            //check if there is getMethod
            try {
                Method m = clazz.getDeclaredMethod("get" + firstCharCap(fieldName));
                if (m != null)
                    return m.invoke(object);
            } catch (Exception e) {
                //    e.printStackTrace();
            }
            //no get method, access field directly
            try {
                Field f = clazz.getDeclaredField(fieldName);
                if (!f.isAccessible())
                    f.setAccessible(true);  // security manager may not be happy about this
                return f.get(object);
            } catch (Exception e) {
                //    e.printStackTrace();
            }
            //move to superclass
            clazz = clazz.getSuperclass();
        }
        throw new NoSuchFieldError(object.getClass() + "." + fieldName);
    }

    public void setFieldValue(String fieldName, Object object, Object value) {
        Class clazz = object.getClass();
        //iterate over class hierarchy, until root class
        while (clazz != Object.class) {
            //check if there is getMethod
            try {
                Method m = clazz.getMethod("set" + firstCharCap(fieldName), value.getClass());
                if (m != null) {
                    m.invoke(object, value);
                    return;
                }
            } catch (Exception e) {
                //    e.printStackTrace();
            }
            //no get method, access field directly
            try {
                Field f = clazz.getDeclaredField(fieldName);
                if (!f.isAccessible())
                    f.setAccessible(true);  // security manager may not be happy about this
                f.set(object, value);
                return;
            } catch (Exception e) {
                //    e.printStackTrace();
            }
            //move to superclass
            clazz = clazz.getSuperclass();
        }
        throw new NoSuchFieldError(object.getClass() + "." + fieldName);

    }


    private String firstCharCap(String s) {
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    public boolean containsClass(Class clazz) {
        for (ClassInfo c : registered) {
            if (c.getName().equals(clazz.getName()))
                return true;
        }
        return false;
    }

    public int getClassId(Class clazz) {
        for (int i = 0; i < registered.size(); i++) {
            if (registered.get(i).getName().equals(clazz.getName()))
                return i;
        }
        throw new Error("Class is not registered: " + clazz);
    }

    public void writeObject(DataOutput out, Object obj, FastArrayList objectStack) throws IOException {
        registerClass(obj.getClass());

        //write class header
        int classId = getClassId(obj.getClass());
        LongPacker.packInt(out, classId);
        ClassInfo classInfo = registered.get(classId);

        ObjectStreamClass streamClass = ObjectStreamClass.lookup(obj.getClass());
        LongPacker.packInt(out, streamClass.getFields().length);

        for (ObjectStreamField f : streamClass.getFields()) {
            //write field ID
            int fieldId = classInfo.getFieldId(f.getName());
            if (fieldId == -1) {
                //field does not exists in class definition stored in db,
                //propably new field was added so add field descriptor
                fieldId = classInfo.addFieldInfo(new FieldInfo(f));
                db.update(serialClassInfoRecid, registered, serializer);
            }
            LongPacker.packInt(out, fieldId);
            //and write value
            Object fieldValue = getFieldValue(f.getName(), obj);
            serialize(out, fieldValue, objectStack);
        }


    }


    public Object readObject(DataInput in, FastArrayList objectStack) throws IOException {
        //read class header
        try {
            int classId = LongPacker.unpackInt(in);
            ClassInfo classInfo = registered.get(classId);
            Class clazz = Class.forName(classInfo.getName());
            assertClassSerializable(clazz);

            Object o = createInstance(clazz, Object.class);
            objectStack.add(o);
            int fieldCount = LongPacker.unpackInt(in);
            for (int i = 0; i < fieldCount; i++) {
                int fieldId = LongPacker.unpackInt(in);
                FieldInfo f = classInfo.getField(fieldId);
                Object fieldValue = deserialize(in, objectStack);
                setFieldValue(f.getName(), o, fieldValue);
            }
            return o;
        } catch (Exception e) {
            throw new Error("Could not instanciate class", e);
        }
    }

    /**
     * Little trick to create new instance without using constructor.
     * Taken from http://www.javaspecialists.eu/archive/Issue175.html
     */
    private static <T> T createInstance(Class<T> clazz, Class<? super T> parent) {

        try {
            //TODO dependecy on nonpublic JVM API
            sun.reflect.ReflectionFactory rf =
                    sun.reflect.ReflectionFactory.getReflectionFactory();
            Constructor objDef = parent.getDeclaredConstructor();
            Constructor intConstr = rf.newConstructorForSerialization(
                    clazz, objDef
            );
            return clazz.cast(intConstr.newInstance());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create object", e);
        }
    }

    protected abstract Object deserialize(DataInput in, FastArrayList objectStack) throws IOException, ClassNotFoundException;

    protected abstract void serialize(DataOutput out, Object fieldValue, FastArrayList objectStack) throws IOException;


}
