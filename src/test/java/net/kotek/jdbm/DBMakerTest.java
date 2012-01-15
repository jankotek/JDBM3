package net.kotek.jdbm;

import junit.framework.TestCase;
import java.io.IOException;

public class DBMakerTest extends TestCaseWithTestFile {
    
    public void testMemory() throws IOException {
        DBStore db = (DBStore) new DBMaker(null)
                .disableCache()
                .build();
        
        long recid = db.insert("aaa");
        db.commit();
        db.update(recid,"bbb");
        db.rollback();
        assertEquals("aaa",db.fetch(recid));
        
        db.close();
        
        db = (DBStore) new DBMaker(null)
                .disableCache()
                .build();

        //this will fail if 'test' already exists
        try{
            db.fetch(recid);
            fail("record should not exist");
        }catch(Throwable e){
            //ignore
        }

    }

    public void testDisk() throws IOException {
        DBStore db = (DBStore) new DBMaker(newTestFile())
                .disableCache()
                .build();

        long recid = db.insert("aaa");
        db.commit();
        db.update(recid,"bbb");
        db.rollback();
        assertEquals("aaa",db.fetch(recid));

        db.close();


    }

}

