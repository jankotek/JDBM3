package net.kotek.jdbm;

import junit.framework.TestCase;


public class StorageDiskMappedTest extends TestCase {
    
    public void testNumberOfPages(){

        assertTrue(StorageDiskMapped.PAGES_PER_FILE * Storage.PAGE_SIZE <Integer.MAX_VALUE);
        
    }
}
