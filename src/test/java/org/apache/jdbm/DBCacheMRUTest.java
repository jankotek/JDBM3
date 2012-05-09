package org.apache.jdbm;

import java.io.IOException;
import java.util.ArrayList;

public class DBCacheMRUTest extends TestCaseWithTestFile {

    public void testPurgeEntryClearsCache() throws IOException {
        DBCacheMRU d = (DBCacheMRU) newDBCache();
        
        for(long i = 0;i<1e3;i++)
            d.addEntry(newEntry(i));

        for(long i = 0;i<1e3;i++)
            d.purgeEntry();

        assertEquals(d._hash.size(),0);
    }
    
    
    DBCacheMRU.CacheEntry newEntry(long i){
        return new DBCacheMRU.CacheEntry(i,i);
    }


    public void testCacheMaxSize() throws IOException {

        DBCacheMRU d = (DBCacheMRU) DBMaker
                .openFile(newTestFile())
                .setMRUCacheSize(100)
                .make();

        ArrayList<Long> recids = new ArrayList<Long>();
        for(int i = 0;i<1e5;i++){
            recids.add(d.insert("aa"+i));
        }
        d.commit();
        for(int i = 0;i<1e5;i++){
            d.fetch(recids.get(i));
        }


        assert(d._hash.size()<=100);


    }
}
