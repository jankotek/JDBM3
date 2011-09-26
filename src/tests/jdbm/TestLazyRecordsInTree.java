package jdbm;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

public class TestLazyRecordsInTree extends TestCaseWithTestFile{

    String makeString(int size){
        StringBuilder s = new StringBuilder(size);
        for(int i=0;i<size;i++){
            s.append('a');
        }
        return s.toString();
    }

    void doIt(BaseRecordManager r, Map<Integer,String> m) throws IOException {
        m.put(1,"");
        long counter = r.countRecords();
        //number of records should increase after inserting big record
        m.put(1,makeString(1000));
        assertEquals(counter+1,r.countRecords());
        assertEquals(m.get(1),makeString(1000));

        //old record should be disposed when replaced with big record
        m.put(1,makeString(1001));
        assertEquals(counter+1,r.countRecords());
        assertEquals(m.get(1),makeString(1001));

        //old record should be disposed when replaced with small record
        m.put(1,"aa");
        assertEquals(counter,r.countRecords());
        assertEquals(m.get(1),"aa");

        //old record should be disposed after deleting
        m.put(1,makeString(1001));
        assertEquals(counter+1,r.countRecords());
        assertEquals(m.get(1),makeString(1001));
        m.remove(1);
        assertTrue(counter>=r.countRecords());
        assertEquals(m.get(1),null);



    }

    public void testBTree() throws IOException {
        BaseRecordManager r = newBaseRecordManager();
        Map<Integer,String> m = r.treeMap("test");
        doIt(r,m);
    }
}
