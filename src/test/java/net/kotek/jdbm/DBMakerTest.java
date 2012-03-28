package net.kotek.jdbm;

import java.io.IOException;
import java.util.Set;

public class DBMakerTest extends TestCaseWithTestFile {
    
    public void testMemory() throws IOException {
        DBStore db = (DBStore) DBMaker.openMemory()
                .disableCache()
                .make();
        
        long recid = db.insert("aaa");
        db.commit();
        db.update(recid,"bbb");
        db.rollback();
        assertEquals("aaa",db.fetch(recid));
        
        db.close();
        
        db = (DBStore) DBMaker.openMemory()
                .disableCache()
                .make();

        //this will fail if 'test' already exists
        try{
            db.fetch(recid);
            fail("record should not exist");
        }catch(Throwable e){
            //ignore
        }

    }

    public void testDisk() throws IOException {
        DBStore db = (DBStore)DBMaker.openFile(newTestFile())
                .disableCache()
                .make();

        long recid = db.insert("aaa");
        db.commit();
        db.update(recid,"bbb");
        db.rollback();
        assertEquals("aaa",db.fetch(recid));

        db.close();
    }


    public void testEncrypt(){
        String file = newTestFile();
        DB db = DBMaker.openFile(file)
                .enableEncryption("password",false)
                .make();
        
        Set l = db.createHashSet("test");
        for(int i = 0;i<10000;i++){
            l.add("aa"+i);
        }
        db.commit();
        db.close();
        db = DBMaker.openFile(file)
                .enableEncryption("password",false)
                .make();
        l = db.getHashSet("test");
        for(int i = 0;i<10000;i++){
            assertTrue(l.contains("aa"+i));
        }
        db.close();
        
       
    }




}

