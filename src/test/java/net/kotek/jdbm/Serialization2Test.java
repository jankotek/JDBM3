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


}