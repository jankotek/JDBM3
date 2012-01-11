package net.kotek.jdbm;

import junit.framework.TestCase;

import java.util.List;

public class DBMakerTest extends TestCase {
    
    public void testMemory(){
        DB db = new DBMaker(null)
                .disableCache()
                .build();
        
        List l = db.createLinkedList("test");
        l.add(1);
        l.add(2);
        assert(l.size() == 2);
        db.rollback();;
        assert(l.size() == 0);
        
        db.close();
        
        db = new DBMaker(null)
                .disableCache()
                .build();

        //this will fail if 'test' already exists
        l = db.createLinkedList("test");

    }
}

