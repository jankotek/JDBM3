package net.kotek.jdbm;


import junit.framework.TestCase;

import java.io.IOException;
import java.util.Map;

public class Serialization2Test extends TestCaseWithTestFile {


    public void test2() throws IOException {
        DB db = newBaseRecordManager();

        Serialization2Bean processView = new Serialization2Bean();

        Map<Object, Object> map =  db.createHashMap("test2");

        map.put("abc", processView);

        db.commit();

        Serialization2Bean retProcessView = (Serialization2Bean)map.get("abc");
        assertEquals(processView, retProcessView);

        db.close();
    }


    public void test3() throws IOException {
        
        String file = newTestFile();
        
        Serialized2DerivedBean att = new Serialized2DerivedBean();
        DB db = new DBMaker(file).disableCache().build();

        Map<Object, Object> map =  db.createHashMap("test");

        map.put("att", att);
        db.commit();
        db.close();
        db = new DBMaker(file).disableCache().build();
        map =  db.getHashMap("test");


        Serialized2DerivedBean retAtt = (Serialized2DerivedBean) map.get("att");
        assertEquals(att, retAtt);
    }

}