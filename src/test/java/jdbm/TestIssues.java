package jdbm;


import java.io.IOException;

public class TestIssues extends TestCaseWithTestFile{


    /*
    test this issue
    http://code.google.com/p/jdbm2/issues/detail?id=2
     */
    public void testHTreeClear() throws IOException {
        final RecordManageAbstract recman = newRecordManager();
        final HTree<String,String> tree = new  HTree(recman);
        recman.setNamedObject("test", tree.getRecid());


        for (int i = 0; i < 1001; i++) {
            tree.put(String.valueOf(i),String.valueOf(i));
        }
        recman.commit();
        System.out.println("finished adding");

        tree.clear();
        recman.commit();
        System.out.println("finished clearing");
        assertTrue(tree.isEmpty());
    }


    public void testBTreeClear() throws IOException {
        final RecordManager recman = newRecordManager();
        final PrimaryTreeMap<String,String> treeMap = recman.createTreeMap("test");

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
