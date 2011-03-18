package jdbm;


import jdbm.htree.HTree;
import jdbm.htree.HTreeMap;
import jdbm.recman.TestCaseWithTestFile;

import java.io.IOException;

public class TestIssues extends TestCaseWithTestFile{


    /*
    test this issue
    http://code.google.com/p/jdbm2/issues/detail?id=2
     */
    public void testHTreeClear() throws IOException {
        final RecordManager recman = newRecordManager();
        final HTree tree = HTree.createInstance(recman);
        recman.setNamedObject("test", tree.getRecid());
        final HTreeMap<String,String> treeMap = tree.asMap();

        for (int i = 0; i < 1001; i++) {
            treeMap.put(String.valueOf(i),String.valueOf(i));
        }
        recman.commit();
        System.out.println("finished adding");

        treeMap.clear();
        recman.commit();
        System.out.println("finished clearing");
        assertTrue(treeMap.isEmpty());
    }


    public void testBTreeClear() throws IOException {
        final RecordManager recman = newRecordManager();
        final PrimaryTreeMap<String,String> treeMap = recman.treeMap("test");

        for (int i = 0; i < 1001; i++) {
            treeMap.put(String.valueOf(i),String.valueOf(i));
        }
        recman.commit();
        System.out.println("finished adding");

        treeMap.clear();
        recman.commit();
        System.out.println("finished clearing");
        assertTrue(treeMap.isEmpty());
    }
}
