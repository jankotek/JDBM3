package org.apache.jdbm;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RollbackTest extends TestCaseWithTestFile{

    public void test_treemap() throws IOException {
        DB db = newDBCache();
        Map<Integer, String> map = db.createTreeMap("collectionName");

        map.put(1, "one");
        map.put(2, "two");

        assertEquals(2, map.size());
        db.commit(); //persist changes into disk

        map.put(3, "three");
        assertEquals(3, map.size());
        db.rollback(); //revert recent changes
        assertEquals(2, map.size());
    }

    public void test_hashmap() throws IOException {
        DB db = newDBCache();
        Map<Integer, String> map = db.createHashMap("collectionName");

        map.put(1, "one");
        map.put(2, "two");

        assertEquals(2, map.size());
        db.commit(); //persist changes into disk

        map.put(3, "three");
        assertEquals(3, map.size());
        db.rollback(); //revert recent changes
        assertEquals(2, map.size());
    }

    public void test_treeset() throws IOException {
        DB db = newDBCache();
        Set<Integer> c = db.createTreeSet("collectionName");

        c.add(1);
        c.add(2);

        assertEquals(2, c.size());
        db.commit(); //persist changes into disk

        c.add(3);
        assertEquals(3, c.size());
        db.rollback(); //revert recent changes
        assertEquals(2, c.size());
    }


    public void test_hashset() throws IOException {
        DB db = newDBCache();
        Set<Integer> c = db.createHashSet("collectionName");

        c.add(1);
        c.add(2);

        assertEquals(2, c.size());
        db.commit(); //persist changes into disk

        c.add(3);
        assertEquals(3, c.size());
        db.rollback(); //revert recent changes
        assertEquals(2, c.size());
    }

    public void test_linkedlist() throws IOException {
        DB db = newDBCache();
        List<Integer> c = db.createLinkedList("collectionName");

        c.add(1);
        c.add(2);

        assertEquals(2, c.size());
        db.commit(); //persist changes into disk

        c.add(3);
        assertEquals(3, c.size());
        db.rollback(); //revert recent changes
        assertEquals(2, c.size());
    }

}
