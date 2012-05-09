package org.apache.jdbm;

import java.util.Map;
import java.util.Set;


public class DBCacheTest extends TestCaseWithTestFile {


    // https://github.com/jankotek/JDBM3/issues/11
    public void test_Issue_11_soft_cache_record_disappear(){
        long MAX = (long) 1e6;

        String file = newTestFile();
        DB d = DBMaker.openFile(file)
                .disableTransactions()
                .enableSoftCache()
                .make();
        
        Set<Integer> set = d.createHashSet("1");
        
        for(Integer i = 0;i<MAX;i++){
            set.add(i);                       
        }
        
        d.close();

        d = DBMaker.openFile(file)
                .disableTransactions()
                .enableSoftCache()
                .make();

        set = d.getHashSet("1");
        for(Integer i = 0;i<MAX;i++){
            assertTrue(set.contains(i));
        }

    }


    public void test_issue_xyz(){
        org.apache.jdbm.DB db = DBMaker.openFile(newTestFile())
                .enableSoftCache()
                .make();
        Map m = db.createTreeMap("test");

        for(int i=0;i<1e5;i++){
            m.put("test"+i,"test"+i);
        }
        db.close();
//
//   problem in cache, throws;
//        java.lang.IllegalArgumentException: Argument 'recid' is invalid: 0
//        at org.apache.jdbm.DBStore.fetch(DBStore.java:356)
//        at org.apache.jdbm.DBCache.fetch(DBCache.java:292)
//        at org.apache.jdbm.BTreeNode.loadNode(BTreeNode.java:833)
//        at org.apache.jdbm.BTreeNode.insert(BTreeNode.java:391)
//        at org.apache.jdbm.BTreeNode.insert(BTreeNode.java:392)
//        at org.apache.jdbm.BTreeNode.insert(BTreeNode.java:392)
//        at org.apache.jdbm.BTree.insert(BTree.java:281)
//        at org.apache.jdbm.BTreeMap.put(BTreeMap.java:285)
//        at org.apache.jdbm.DBCacheTest.test_some_random_shit(DBCacheTest.java:48)
//

    }
}
