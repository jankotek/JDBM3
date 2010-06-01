/*
 *  $Id: TestRecordManager.java,v 1.6 2003/09/21 15:49:02 boisvert Exp $
 *
 *  Unit tests for RecordManager class
 *
 *  Simple db toolkit
 *  Copyright (C) 1999, 2000 Cees de Groot <cg@cdegroot.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Library General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Library General Public License for more details.
 *
 *  You should have received a copy of the GNU Library General Public License
 *  along with this library; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 */
package jdbm.recman;

import java.util.ArrayList;
import java.util.Properties;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.RecordManagerOptions;
import junit.framework.TestSuite;

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
        byte[] data = TestUtil.makeRecord(10000, (byte) 1);
        long rowid = recman.insert(data);
        assertTrue("check data1",
               TestUtil.checkRecord( (byte[]) recman.fetch(rowid), 10000, (byte) 1) );

        // update it as a 20,000 byte record.
        data = TestUtil.makeRecord(20000, (byte) 2);
        recman.update(rowid, data);
        assertTrue("check data2",
               TestUtil.checkRecord( (byte[])recman.fetch(rowid), 20000, (byte) 2) );

        // insert a third record.
        data = TestUtil.makeRecord(20, (byte) 3);
        long rowid2 = recman.insert(data);
        assertTrue("check data3",
               TestUtil.checkRecord( (byte[]) recman.fetch(rowid2), 20, (byte) 3) );

        // now, grow the first record again
        data = TestUtil.makeRecord(30000, (byte) 4);
        recman.update(rowid, data);
        assertTrue("check data4",
               TestUtil.checkRecord( (byte[]) recman.fetch(rowid), 30000, (byte) 4) );


        // delete the record
        recman.delete(rowid);

        // close the file
        recman.close();
    }
    
    /**
     *  Test delete and immediate reuse. This attempts to reproduce
     *  a bug in the stress test involving 0 record lengths.
     */
    public void testCompress() 
        throws Exception
    {
    	Properties props = new Properties();
    	props.put(RecordManagerOptions.COMPRESS, "TRUE");
    	    
        RecordManager recman = RecordManagerFactory. createRecordManager(
        			newTestFile(), props);
        
        //create huge list to be compressed
        ArrayList l1 = new ArrayList();
        for(int i = 0;i<100000;i++) l1.add(i);
        
        long id = recman.insert(l1);
        recman.commit();
        ArrayList l2 = (ArrayList) recman.fetch(id);
        assertEquals(l1,l2);

        
        recman.update(id, l2);
        recman.commit();
        ArrayList l3 = (ArrayList) recman.fetch(id);
        assertEquals(l1,l3);

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
        byte[] data = TestUtil.makeRecord(1500, (byte) 1);
        long rowid = recman.insert(data);
        assertTrue("check data1",
               TestUtil.checkRecord( (byte[]) recman.fetch(rowid), 1500, (byte) 1) );


        // delete the record
        recman.delete(rowid);

        // insert a 0 byte record. Should have the same rowid.
        data = TestUtil.makeRecord(0, (byte) 2);
        long rowid2 = recman.insert(data);
        assertEquals("old and new rowid", rowid, rowid2);
        assertTrue("check data2",
               TestUtil.checkRecord( (byte[]) recman.fetch(rowid2), 0, (byte) 2) );

        // now make the record a bit bigger
        data = TestUtil.makeRecord(10000, (byte) 3);
        recman.update(rowid, data);
        assertTrue("check data3",
               TestUtil.checkRecord( (byte[]) recman.fetch(rowid), 10000, (byte) 3) );

        // .. and again
        data = TestUtil.makeRecord(30000, (byte) 4);
        recman.update(rowid, data);
        assertTrue("check data3",
               TestUtil.checkRecord( (byte[]) recman.fetch(rowid), 30000, (byte) 4) );

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

        // insert a 150000 byte record.
        byte[] data1 = TestUtil.makeRecord(150000, (byte) 1);
        long rowid1 = recman.insert(data1);
        assertTrue("check data1",
               TestUtil.checkRecord( (byte[]) recman.fetch(rowid1), 150000, (byte) 1) );

        // rollback transaction, should revert to previous state
        recman.rollback();

        // insert same 150000 byte record.
        byte[] data2 = TestUtil.makeRecord(150000, (byte) 1);
        long rowid2 = recman.insert(data2);
        assertTrue("check data2",
               TestUtil.checkRecord( (byte[]) recman.fetch(rowid2), 150000, (byte) 1) );

        assertEquals("old and new rowid", rowid1, rowid2);

        recman.commit();

        // insert a 150000 byte record.
        data1 = TestUtil.makeRecord(150000, (byte) 2);
        rowid1 = recman.insert(data1);
        assertTrue("check data1",
               TestUtil.checkRecord( (byte[]) recman.fetch(rowid1), 150000, (byte) 2) );

        // rollback transaction, should revert to previous state
        recman.rollback();

        // insert same 150000 byte record.
        data2 = TestUtil.makeRecord(150000, (byte) 2);
        rowid2 = recman.insert(data2);
        assertTrue("check data2",
               TestUtil.checkRecord( (byte[]) recman.fetch(rowid2), 150000, (byte) 2) );

        assertEquals("old and new rowid", rowid1, rowid2);

        // close the file
        recman.close();
    }

    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(new TestSuite(TestRecordManager.class));
    }
}
