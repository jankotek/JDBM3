package jdbm;

import java.io.IOException;
import java.util.*;

public class DefragTest extends TestCaseWithTestFile {


    public void testDefrag1() throws IOException {
        String file = newTestFile();
        DBStore m = new DBStore(file, false, false);
        long loc = m.insert("123");
        m.defrag();
        m.close();
        m = new DBStore(file, false, false);
        assertEquals(m.fetch(loc), "123");
    }


    public void testDefrag2() throws IOException {
        String file = newTestFile();
        DBStore m = new DBStore(file, false, false);
        TreeMap<Long, String> map = new TreeMap<Long, String>();
        for (int i = 0; i < 10000; i++) {
            long loc = m.insert("" + i);
            map.put(loc, "" + i);
        }

        m.defrag();
        m.close();
        m = new DBStore(file, false, false);
        for (Long l : map.keySet()) {
            String val = map.get(l);
            assertEquals(val, m.fetch(l));
        }
    }


    public void testDefragBtree() throws IOException {
        String file = newTestFile();
        DBStore m = new DBStore(file, false, false);
        Map t = m.createTreeMap("aa");
        TreeMap t2 = new TreeMap();
        for (int i = 0; i < 10000; i++) {
            t.put(i, "" + i);
            t2.put(i, "" + i);
        }

        m.defrag();
        m.close();
        m = new DBStore(file, false, false);
        t = m.loadTreeMap("aa");
        assertEquals(t, t2);
    }

    public void testDefragLinkedList() throws Exception {
        String file = newTestFile();
        DBStore r = new DBStore(file, false, false);
        List l = r.createLinkedList("test");
        Map<Long, Double> junk = new LinkedHashMap<Long, Double>();

        for (int i = 0; i < 1e4; i++) {
            //insert some junk
            Double d = Math.random();
            l.add(d);
            junk.put(r.insert(d), d);
        }
        r.commit();
        //make copy of linked list
        List l2 = new ArrayList(l);
        long oldRecCount = r.countRecords();
        r.defrag();

        r.close();
        r = new DBStore(file, false, false);
        assertEquals(oldRecCount, r.countRecords());

        //compare that list was unchanged
        assertEquals(l2, new ArrayList(r.loadLinkedList("test")));

        //and check that random junk still have the same recids
        for (Long recid : junk.keySet()) {
            assertEquals(junk.get(recid), r.fetch(recid));
        }

        r.close();
    }
}
