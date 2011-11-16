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

package jdbm;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *  This class contains all Unit tests for {@link RecordManager}.
 */
public class TestRecordManager extends TestCaseWithTestFile {


    /**
     *  Test constructor
     */
    public void testCtor()
        throws Exception
    {
        RecordManager recman;

        recman = newRecordManager();
        recman.close();
    }

    /**
     *  Test basics
     */
    public void testBasics()
        throws Exception
    {
        RecordManager recman;

        recman = newRecordManager();

        // insert a 10,000 byte record.
        byte[] data = UtilTT.makeRecord(10000, (byte) 1);
        long rowid = recman.insert(data);
        assertTrue("check data1",
               UtilTT.checkRecord((byte[]) recman.fetch(rowid), 10000, (byte) 1) );

        // update it as a 20,000 byte record.
        data = UtilTT.makeRecord(20000, (byte) 2);
        recman.update(rowid, data);
        assertTrue("check data2",
               UtilTT.checkRecord((byte[]) recman.fetch(rowid), 20000, (byte) 2) );

        // insert a third record.
        data = UtilTT.makeRecord(20, (byte) 3);
        long rowid2 = recman.insert(data);
        assertTrue("check data3",
               UtilTT.checkRecord((byte[]) recman.fetch(rowid2), 20, (byte) 3) );

        // now, grow the first record again
        data = UtilTT.makeRecord(30000, (byte) 4);
        recman.update(rowid, data);
        assertTrue("check data4",
               UtilTT.checkRecord((byte[]) recman.fetch(rowid), 30000, (byte) 4) );


        // delete the record
        recman.delete(rowid);

        // close the file
        recman.close();
    }
    


    /**
     *  Test delete and immediate reuse. This attempts to reproduce
     *  a bug in the stress test involving 0 record lengths.
     */
    public void testDeleteAndReuse() 
        throws Exception
    {
        RecordManager recman;

        recman = newRecordManager();

        // insert a 1500 byte record.
        byte[] data = UtilTT.makeRecord(1500, (byte) 1);
        long rowid = recman.insert(data);
        assertTrue("check data1",
               UtilTT.checkRecord((byte[]) recman.fetch(rowid), 1500, (byte) 1) );


        // delete the record
        recman.delete(rowid);

        // insert a 0 byte record. Should have the same rowid.
        data = UtilTT.makeRecord(0, (byte) 2);
        long rowid2 = recman.insert(data);
        assertEquals("old and new rowid", rowid, rowid2);
        assertTrue("check data2",
               UtilTT.checkRecord((byte[]) recman.fetch(rowid2), 0, (byte) 2) );

        // now make the record a bit bigger
        data = UtilTT.makeRecord(10000, (byte) 3);
        recman.update(rowid, data);
        assertTrue("check data3",
               UtilTT.checkRecord((byte[]) recman.fetch(rowid), 10000, (byte) 3) );

        // .. and again
        data = UtilTT.makeRecord(30000, (byte) 4);
        recman.update(rowid, data);
        assertTrue("check data3",
               UtilTT.checkRecord((byte[]) recman.fetch(rowid), 30000, (byte) 4) );

        // close the file
        recman.close();
    }

    /**
     *  Test rollback sanity. Attemts to add a new record, rollback and
     *  add the same record.  We should obtain the same record id for both
     *  operations.
     */
    public void testRollback() 
        throws Exception
    {
        RecordManager recman;

        // Note: We start out with an empty file
        recman = newRecordManager();

        recman.insert(""); //first insert an empty record, to make sure serializer is initialized
        recman.commit();
        // insert a 150000 byte record.
        byte[] data1 = UtilTT.makeRecord(150000, (byte) 1);
        long rowid1 = recman.insert(data1);
        assertTrue("check data1",
               UtilTT.checkRecord((byte[]) recman.fetch(rowid1), 150000, (byte) 1) );

        // rollback transaction, should revert to previous state
        recman.rollback();

        // insert same 150000 byte record.
        byte[] data2 = UtilTT.makeRecord(150000, (byte) 1);
        long rowid2 = recman.insert(data2);
        assertTrue("check data2",
               UtilTT.checkRecord((byte[]) recman.fetch(rowid2), 150000, (byte) 1) );

        assertEquals("old and new rowid", rowid1, rowid2);

        recman.commit();

        // insert a 150000 byte record.
        data1 = UtilTT.makeRecord(150000, (byte) 2);
        rowid1 = recman.insert(data1);
        assertTrue("check data1",
               UtilTT.checkRecord((byte[]) recman.fetch(rowid1), 150000, (byte) 2) );

        // rollback transaction, should revert to previous state
        recman.rollback();

        // insert same 150000 byte record.
        data2 = UtilTT.makeRecord(150000, (byte) 2);
        rowid2 = recman.insert(data2);
        assertTrue("check data2",
               UtilTT.checkRecord((byte[]) recman.fetch(rowid2), 150000, (byte) 2) );

        assertEquals("old and new rowid", rowid1, rowid2);

        // close the file
        recman.close();
    }

    
    public void testNonExistingRecid() throws IOException{
    	RecordManager recman = newRecordManager();
    	
    	Object obj = recman.fetch(6666666);
    	assertTrue(obj == null);
    	
    	try{
    		recman.update(6666666, obj);
    		recman.commit();
    		fail();
    	}catch(IOException expected){

    	}
    }
    
    public void testTreeMapValueSerializer() throws Exception{
    	final AtomicInteger i = new AtomicInteger(0);
    	Serializer<String> ser = new Serializer<String>(){

			public String deserialize(DataInput in) throws IOException, ClassNotFoundException {
				i.incrementAndGet();
				return in.readUTF();
			}

			public void serialize(DataOutput out, String obj) throws IOException {
				i.incrementAndGet();
				out.writeUTF(obj);
			}};
			
		RecordManager recman = newRecordManager();
		PrimaryTreeMap<Long,String> t =  recman.createTreeMap("test", null, ser,null);
		t.put(1l, "hopsa hejsa1");
		t.put(2l, "hopsa hejsa2");
		recman.commit();
		assertEquals(t.get(2l),"hopsa hejsa2");
		assertTrue(i.intValue()>0);
    }

    public void testCountRecid() throws Exception{
        BaseRecordManager recman = newBaseRecordManager();
        recman.insert(""); //first insert an empty record, to make sure serializer is initialized
        long baseCount = recman.countRecords();
        for(int i = 1;i<3000;i++){
            Object val = "qjiodjqwoidjqwiodoi";

            recman.insert(val);
            if(i%1000==0) recman.commit();

            assertEquals(recman.countRecords(),i+baseCount);
        }

    }

}
