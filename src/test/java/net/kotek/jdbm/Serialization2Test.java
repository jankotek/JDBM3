package net.kotek.jdbm;


import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class Serialization2Test extends TestCaseWithTestFile {


    public void test2() throws IOException {
        DB db = newDBNoCache();

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



    static class AAA implements Serializable {
        String test  = "aa";
    }


    public void testReopenWithDefrag(){

        String f = newTestFile();

        DB db = new DBMaker(f)
                .disableTransactions()
                .build();

        Map<Integer,AAA> map = db.createTreeMap("test");
        map.put(1,new AAA());

        db.defrag(true);
        db.close();

        db = new DBMaker(f)
                .disableTransactions()
                .build();

        map = db.getTreeMap("test");
        assertNotNull(map.get(1));
        assertEquals(map.get(1).test, "aa");


        db.close();
    }

}