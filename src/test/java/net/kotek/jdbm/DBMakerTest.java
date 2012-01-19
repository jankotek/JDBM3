package net.kotek.jdbm;

import junit.framework.TestCase;
import java.io.IOException;
import java.util.List;
import java.util.Set;

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


    public void testEncrypt(){
        String file = newTestFile();
        DB db = new DBMaker(file)
                .enableEncryption("password",false)
                .build();
        
        Set l = db.createHashSet("test");
        for(int i = 0;i<10000;i++){
            l.add("aa"+i);
        }
        db.commit();
        db.close();
        db = new DBMaker(file)
                .enableEncryption("password",false)
                .build();
        l = db.getHashSet("test");
        for(int i = 0;i<10000;i++){
            assertTrue(l.contains("aa"+i));
        }
        db.close();
        
       
    }

}

