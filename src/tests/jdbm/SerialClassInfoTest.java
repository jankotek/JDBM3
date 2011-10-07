package jdbm;

import junit.framework.TestCase;

public class SerialClassInfoTest extends TestCase {

    static class Bean1{
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
    }

    static class Bean2 extends Bean1{
        private String field3 = null;
        Bean2(String field1, String field2, String field3){
            super(field1,field2);
            this.field3 = field3;
        }
    }


    SerialClassInfo s = new SerialClassInfo();
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



}
