package net.kotek.jdbm;


import java.io.IOException;
import java.util.Map;

public class TestIssues extends TestCaseWithTestFile {


    /*
    test this issue
    http://code.google.com/p/jdbm2/issues/detail?id=2
     */
    public void testHTreeClear() throws IOException {
        final DBAbstract db = newDBCache();
        final HTree<String, String> tree = (HTree) db.createHashMap("name");

        for (int i = 0; i < 1001; i++) {
            tree.put(String.valueOf(i), String.valueOf(i));
        }
        db.commit();
        System.out.println("finished adding");

        tree.clear();
        db.commit();
        System.out.println("finished clearing");
        assertTrue(tree.isEmpty());
    }


    public void testBTreeClear() throws IOException {
        final DB db = newDBCache();
        final Map<String, String> treeMap = db.createTreeMap("test");

        for (int i = 0; i < 1001; i++) {
            treeMap.put(String.valueOf(i), String.valueOf(i));
        }
        db.commit();
        System.out.println("finished adding");

        treeMap.clear();
        db.commit();
        System.out.println("finished clearing");
        assertTrue(treeMap.isEmpty());
    }


    public void test_issue_17_double_concurrent_get() throws InterruptedException {

        final DB db = new DBMaker(newTestFile()).disableTransactions().disableCache().build();
        db.createHashMap("map");

        class RR implements Runnable{

            public void run() {
                Map<Integer, String> m =db.getHashMap("map");

                for(int i = 1; i < 10000; i++)
                    m.put(i,  "-"+ i );

            }
        }

        Thread thread = new Thread(new RR());

        thread.start();
        new RR().run();

        thread.join();
        db.close();
    }
}
