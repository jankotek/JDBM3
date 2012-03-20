package net.kotek.jdbm;

import java.io.IOException;

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
}
