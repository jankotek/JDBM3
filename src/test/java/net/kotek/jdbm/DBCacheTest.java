package net.kotek.jdbm;

import java.util.Map;
import java.util.Set;


public class DBCacheTest extends TestCaseWithTestFile {


    // https://github.com/jankotek/JDBM3/issues/11
    public void test_Issue_11_soft_cache_record_disappear(){
        long MAX = (long) 1e6;

        String file = newTestFile();
        DB d = new DBMaker(file)
                .disableTransactions()
                .enableSoftCache()
                .build();
        
        Set<Integer> set = d.createHashSet("1");
        
        for(Integer i = 0;i<MAX;i++){
            set.add(i);                       
        }
        
        d.close();

        d = new DBMaker(file)
                .disableTransactions()
                .enableSoftCache()
                .build();

        set = d.getHashSet("1");
        for(Integer i = 0;i<MAX;i++){
            assertTrue(set.contains(i));
        }

    }


    public void test_issue_xyz(){
        net.kotek.jdbm.DB db = new DBMaker(newTestFile())
                .enableSoftCache()
                .build();
        Map m = db.createTreeMap("test");

        for(int i=0;i<1e5;i++){
            m.put("test"+i,"test"+i);
        }
        db.close();
//
//   problem in cache, throws;
//        java.lang.IllegalArgumentException: Argument 'recid' is invalid: 0
//        at net.kotek.jdbm.DBStore.fetch(DBStore.java:356)
//        at net.kotek.jdbm.DBCache.fetch(DBCache.java:292)
//        at net.kotek.jdbm.BTreeNode.loadNode(BTreeNode.java:833)
//        at net.kotek.jdbm.BTreeNode.insert(BTreeNode.java:391)
//        at net.kotek.jdbm.BTreeNode.insert(BTreeNode.java:392)
//        at net.kotek.jdbm.BTreeNode.insert(BTreeNode.java:392)
//        at net.kotek.jdbm.BTree.insert(BTree.java:281)
//        at net.kotek.jdbm.BTreeMap.put(BTreeMap.java:285)
//        at net.kotek.jdbm.DBCacheTest.test_some_random_shit(DBCacheTest.java:48)
//

    }
}
