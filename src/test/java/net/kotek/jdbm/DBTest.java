/*******************************************************************************
 * Copyright 2010 Cees De Groot, Alex Boisvert, Jan Kotek
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package net.kotek.jdbm;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class contains all Unit tests for {@link DBAbstract}.
 */
public class DBTest extends TestCaseWithTestFile {


    /**
     * Test constructor
     */
    public void testCtor()
            throws Exception {
        DB db;

        db = newRecordManager();
        db.close();
    }

    /**
     * Test basics
     */
    public void testBasics()
            throws Exception {

        DBAbstract db = newRecordManager();

        // insert a 10,000 byte record.
        byte[] data = UtilTT.makeRecord(10000, (byte) 1);
        long rowid = db.insert(data);
        assertTrue("check data1",
                UtilTT.checkRecord((byte[]) db.fetch(rowid), 10000, (byte) 1));

        // update it as a 20,000 byte record.
        data = UtilTT.makeRecord(20000, (byte) 2);
        db.update(rowid, data);
        assertTrue("check data2",
                UtilTT.checkRecord((byte[]) db.fetch(rowid), 20000, (byte) 2));

        // insert a third record.
        data = UtilTT.makeRecord(20, (byte) 3);
        long rowid2 = db.insert(data);
        assertTrue("check data3",
                UtilTT.checkRecord((byte[]) db.fetch(rowid2), 20, (byte) 3));

        // now, grow the first record again
        data = UtilTT.makeRecord(30000, (byte) 4);
        db.update(rowid, data);
        assertTrue("check data4",
                UtilTT.checkRecord((byte[]) db.fetch(rowid), 30000, (byte) 4));


        // delete the record
        db.delete(rowid);

        // close the file
        db.close();
    }


    /**
     * Test delete and immediate reuse. This attempts to reproduce
     * a bug in the stress test involving 0 record lengths.
     */
    public void testDeleteAndReuse()
            throws Exception {

        DBAbstract db = newRecordManager();

        // insert a 1500 byte record.
        byte[] data = UtilTT.makeRecord(1500, (byte) 1);
        long rowid = db.insert(data);
        assertTrue("check data1",
                UtilTT.checkRecord((byte[]) db.fetch(rowid), 1500, (byte) 1));


        // delete the record
        db.delete(rowid);

        // insert a 0 byte record. Should have the same rowid.
        data = UtilTT.makeRecord(0, (byte) 2);
        long rowid2 = db.insert(data);
        assertEquals("old and new rowid", rowid, rowid2);
        assertTrue("check data2",
                UtilTT.checkRecord((byte[]) db.fetch(rowid2), 0, (byte) 2));

        // now make the record a bit bigger
        data = UtilTT.makeRecord(10000, (byte) 3);
        db.update(rowid, data);
        assertTrue("check data3",
                UtilTT.checkRecord((byte[]) db.fetch(rowid), 10000, (byte) 3));

        // .. and again
        data = UtilTT.makeRecord(30000, (byte) 4);
        db.update(rowid, data);
        assertTrue("check data3",
                UtilTT.checkRecord((byte[]) db.fetch(rowid), 30000, (byte) 4));

        // close the file
        db.close();
    }

    /**
     * Test rollback sanity. Attemts to add a new record, rollback and
     * add the same record.  We should obtain the same record id for both
     * operations.
     */
    public void testRollback()
            throws Exception {

        // Note: We start out with an empty file
        DBAbstract db = newRecordManager();

        db.insert(""); //first insert an empty record, to make sure serializer is initialized
        db.commit();
        // insert a 150000 byte record.
        byte[] data1 = UtilTT.makeRecord(150000, (byte) 1);
        long rowid1 = db.insert(data1);
        assertTrue("check data1",
                UtilTT.checkRecord((byte[]) db.fetch(rowid1), 150000, (byte) 1));

        // rollback transaction, should revert to previous state
        db.rollback();

        // insert same 150000 byte record.
        byte[] data2 = UtilTT.makeRecord(150000, (byte) 1);
        long rowid2 = db.insert(data2);
        assertTrue("check data2",
                UtilTT.checkRecord((byte[]) db.fetch(rowid2), 150000, (byte) 1));

        assertEquals("old and new rowid", rowid1, rowid2);

        db.commit();

        // insert a 150000 byte record.
        data1 = UtilTT.makeRecord(150000, (byte) 2);
        rowid1 = db.insert(data1);
        assertTrue("check data1",
                UtilTT.checkRecord((byte[]) db.fetch(rowid1), 150000, (byte) 2));

        // rollback transaction, should revert to previous state
        db.rollback();

        // insert same 150000 byte record.
        data2 = UtilTT.makeRecord(150000, (byte) 2);
        rowid2 = db.insert(data2);
        assertTrue("check data2",
                UtilTT.checkRecord((byte[]) db.fetch(rowid2), 150000, (byte) 2));

        assertEquals("old and new rowid", rowid1, rowid2);

        // close the file
        db.close();
    }


    public void testNonExistingRecid() throws IOException {
        DBAbstract db = newRecordManager();

        Object obj = db.fetch(6666666);
        assertTrue(obj == null);

        try {
            db.update(6666666, obj);
            db.commit();
            fail();
        } catch (IOError expected) {

        }
    }

    final static AtomicInteger i = new AtomicInteger(0);

    public static class Serial implements Serializer<String>,Serializable {

        public String deserialize(DataInput in) throws IOException, ClassNotFoundException {
            i.incrementAndGet();
            return in.readUTF();
        }

        public void serialize(DataOutput out, String obj) throws IOException {
            i.incrementAndGet();
            out.writeUTF(obj);
        }
    }

    public void testTreeMapValueSerializer() throws Exception {
        i.set(0);
        Serializer<String> ser = new Serial();

        DB db = newRecordManager();
        Map<Long, String> t = db.<Long, String>createTreeMap("test", null, null, ser);
        t.put(1l, "hopsa hejsa1");
        t.put(2l, "hopsa hejsa2");
        db.commit();
        assertEquals(t.get(2l), "hopsa hejsa2");
        assertTrue(i.intValue() > 0);
    }

    public void testCountRecid() throws Exception {
        DBStore db = newBaseRecordManager();
        db.insert(""); //first insert an empty record, to make sure serializer is initialized
        long baseCount = ((RecordManagerNative)db.recman).countRecords();
        for (int i = 1; i < 3000; i++) {
            Object val = "qjiodjqwoidjqwiodoi";

            db.insert(val);
            if (i % 1000 == 0) db.commit();

            assertEquals(((RecordManagerNative)db.recman).countRecords(), i + baseCount);
        }

    }

    public void testGetCollections() throws IOException {
        DB db = newRecordManager();
        db.createTreeMap("treemap");
        db.createHashMap("hashmap");
        db.createTreeSet("treeset");
        db.createHashSet("hashset");

        db.createLinkedList("linkedlist");
        Map<String, Object>cols = db.getCollections();
        assertTrue(cols.get("treemap") instanceof SortedMap);
        assertTrue(cols.get("hashmap") instanceof Map);

        assertTrue(cols.get("treeset") instanceof SortedSet);
        assertTrue(cols.get("hashset") instanceof Set);
        assertTrue(cols.get("linkedlist") instanceof List);
    }
    
    public void testRegisterShutdown(){
        DB d = new DBMaker(newTestFile()).closeOnExit().build();
        //do nothing
    }
    
    public void testDeleteAfterExit(){
        String f = newTestFile();
        File f1 = new File(StorageDiskMapped.makeFileName(f,1,0));
        File f2 = new File(StorageDiskMapped.makeFileName(f,-1,0));
        
        assertFalse(f1.exists());
        assertFalse(f2.exists());
        
        DB d = new DBMaker(f).deleteFilesAfterClose().build();
        d.createHashSet("test");
        assertTrue(f1.exists());
        assertTrue(f2.exists());
        d.close();
        assertFalse(f1.exists());
        assertFalse(f2.exists());

                
    }

    public void testDeleteAfterExitRAF(){
        String f = newTestFile();
        File f1 = new File(StorageDiskMapped.makeFileName(f,1,0));
        File f2 = new File(StorageDiskMapped.makeFileName(f,-1,0));

        assertFalse(f1.exists());
        assertFalse(f2.exists());

        DB d = new DBMaker(f).deleteFilesAfterClose().useRandomAccessFile().build();
        d.createHashSet("test");
        assertTrue(f1.exists());
        assertTrue(f2.exists());
        d.close();
        assertFalse(f1.exists());
        assertFalse(f2.exists());


    }


}

