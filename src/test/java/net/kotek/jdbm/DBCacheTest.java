package net.kotek.jdbm;

import junit.framework.TestCase;

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
}
