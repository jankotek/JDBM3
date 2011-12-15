package jdbm;

import java.io.*;
import java.util.AbstractMap;
import java.util.ArrayList;

public class SerialClassInfoTest extends TestCaseWithTestFile {

    static class Bean1 implements Serializable {

        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Bean1 bean1 = (Bean1) o;

            if (Double.compare(bean1.doubleField, doubleField) != 0) return false;
            if (Float.compare(bean1.floatField, floatField) != 0) return false;
            if (intField != bean1.intField) return false;
            if (longField != bean1.longField) return false;
            if (field1 != null ? !field1.equals(bean1.field1) : bean1.field1 != null) return false;
            if (field2 != null ? !field2.equals(bean1.field2) : bean1.field2 != null) return false;

            return true;
        }


        protected String field1 = null;
        protected String field2 = null;

        protected int intField = Integer.MAX_VALUE;
        protected long longField = Long.MAX_VALUE;
        protected double doubleField = Double.MAX_VALUE;
        protected float floatField = Float.MAX_VALUE;

        transient int getCalled = 0;
        transient int setCalled = 0;

        public String getField2(){
            getCalled++;
            return field2;
        }

        public void setField2(String field2){
            setCalled++;
            this.field2 = field2;
        }

        Bean1(String field1, String field2){
            this.field1 = field1;
            this.field2 = field2;
        }
        Bean1(){}
    }

    static class Bean2 extends Bean1{

        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;

            Bean2 bean2 = (Bean2) o;

            if (field3 != null ? !field3.equals(bean2.field3) : bean2.field3 != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return field3 != null ? field3.hashCode() : 0;
        }

        private String field3 = null;
        Bean2(String field1, String field2, String field3){
            super(field1,field2);
            this.field3 = field3;
        }
        Bean2(){}
    }


    SerialClassInfo s;

    public void setUp() throws IOException {
        s = new Serialization(null,0);
    }

    Bean1 b = new Bean1("aa","bb");
    Bean2 b2 = new Bean2("aa","bb","cc");

    public void testGetFieldValue1() throws Exception {
        assertEquals("aa",s.getFieldValue("field1",b));
    }
    public void testGetFieldValue2() throws Exception {
        assertEquals("bb",s.getFieldValue("field2",b));
        assertEquals(1,b.getCalled);
    }

    public void testGetFieldValue3() throws Exception {
        assertEquals("aa",s.getFieldValue("field1",b2));
    }

    public void testGetFieldValue4() throws Exception {
        assertEquals("bb",s.getFieldValue("field2",b2));
        assertEquals(1,b2.getCalled);
    }

    public void testGetFieldValue5() throws Exception {
        assertEquals("cc",s.getFieldValue("field3",b2));
    }

    public void testSetFieldValue1(){
        s.setFieldValue("field1",b,"zz");
        assertEquals("zz",b.field1);
    }

    public void testSetFieldValue2(){
        s.setFieldValue("field2",b,"zz");
        assertEquals("zz",b.field2);
        assertEquals(1,b.setCalled);
    }

    public void testSetFieldValue3(){
        s.setFieldValue("field1",b2,"zz");
        assertEquals("zz",b2.field1);
    }

    public void testSetFieldValue4(){
        s.setFieldValue("field2",b2,"zz");
        assertEquals("zz",b2.field2);
        assertEquals(1,b2.setCalled);
    }

    public void testSetFieldValue5(){
        s.setFieldValue("field3",b2,"zz");
        assertEquals("zz",b2.field3);
    }

    public void testGetPrimitiveField(){
        assertEquals(Integer.MAX_VALUE,s.getFieldValue("intField",b2));
        assertEquals(Long.MAX_VALUE,s.getFieldValue("longField",b2));
        assertEquals(Double.MAX_VALUE,s.getFieldValue("doubleField",b2));
        assertEquals(Float.MAX_VALUE,s.getFieldValue("floatField",b2));
    }


    public void testSetPrimitiveField(){
        s.setFieldValue("intField",b2,-1);
        assertEquals(-1,s.getFieldValue("intField",b2));
        s.setFieldValue("longField",b2,-1L);
        assertEquals(-1L,s.getFieldValue("longField",b2));
        s.setFieldValue("doubleField",b2,-1D);
        assertEquals(-1D,s.getFieldValue("doubleField",b2));
        s.setFieldValue("floatField",b2,-1F);
        assertEquals(-1F,s.getFieldValue("floatField",b2));
    }


    <E>E serialize(E e) throws ClassNotFoundException, IOException {
        Serialization s2 = new Serialization(null,0);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        s2.serialize(new DataOutputStream(out),e);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        return  (E) s2.deserialize(new DataInputStream(in));

    }

    public void testSerializable() throws Exception {

        assertEquals(serialize(b),b);
    }


    public void testRecursion() throws Exception{
        AbstractMap.SimpleEntry b = new AbstractMap.SimpleEntry("abcd",null);
        b.setValue(b.getKey());

        AbstractMap.SimpleEntry bx = serialize(b);
        assertEquals(bx,b);
        assert(bx.getKey() == bx.getValue());

    }

    public void testRecursion2() throws Exception{
        AbstractMap.SimpleEntry b = new AbstractMap.SimpleEntry("abcd",null);
        b.setValue(b);

        AbstractMap.SimpleEntry bx = serialize(b);
        assertTrue(bx == bx.getValue());
        assertEquals(bx.getKey(), "abcd");

    }



    public void testRecursion3() throws Exception{
        ArrayList l = new ArrayList();
        l.add("123");
        l.add(l);

        ArrayList l2 = serialize(l);

        assertTrue(l.size()==2);
        assertEquals(l.get(0),"123");
        assertTrue(l.get(1)==l);
    }

    public void testPersisted() throws Exception{
        Bean1 b1 = new Bean1("abc","dcd");
        String f = newTestFile();
        RecordManager r1 = new RecordManagerBuilder(f).build();
        long recid = r1.insert(b1);
        r1.commit();
        r1.close();

        RecordManager r2 = new RecordManagerBuilder(f).build();

        Bean1 b2 = (Bean1) r2.fetch(recid);
        r2.close();
        assertEquals(b1,b2);

    }

}
