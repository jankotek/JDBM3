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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class contains all Unit tests for {@link PhysicalRowIdManager}.
 */
public class PhysicalRowIdManagerTest extends TestCaseWithTestFile {


    private  byte[] data = new byte[100000];

    /**
     * Test constructor
     */
    public void testCtor() throws Exception {
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        RecordFile free = newRecordFile();
        PageManager pmfree = new PageManager(free);

        PhysicalRowIdManager physMgr = new PhysicalRowIdManager(f, pm, new PhysicalRowIdPageManagerFree(free, pmfree));

        f.forceClose();
    }

    /**
     * Test basics
     */
    public void testBasics() throws Exception {

        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        RecordFile free = newRecordFile();
        PageManager pmfree = new PageManager(free);
        PhysicalRowIdManager physMgr = new PhysicalRowIdManager(f, pm, new PhysicalRowIdPageManagerFree(free, pmfree));

        // insert a 10,000 byte record.
        byte[] data = UtilTT.makeRecord(10000, (byte) 1);
        long loc = physMgr.insert(data, 0, data.length);
        DataInputOutput a1 = new DataInputOutput();
        physMgr.fetch(a1, loc);
        assertTrue("check data1",
                UtilTT.checkRecord(a1.toByteArray(), 10000, (byte) 1));

        // update it as a 20,000 byte record.
        data = UtilTT.makeRecord(20000, (byte) 2);
        long loc2 = physMgr.update(loc, data, 0, data.length);
        DataInputOutput a2 = new DataInputOutput();
        physMgr.fetch(a2, loc2);
        assertTrue("check data2",
                UtilTT.checkRecord(a2.toByteArray(), 20000, (byte) 2));

        // insert a third record. This'll effectively block the first one
        // from growing
        data = UtilTT.makeRecord(20, (byte) 3);
        long loc3 = physMgr.insert(data, 0, data.length);
        DataInputOutput a3 = new DataInputOutput();
        physMgr.fetch(a3, loc3);
        assertTrue("check data3",
                UtilTT.checkRecord(a3.toByteArray(), 20, (byte) 3));

        // now, grow the first record again
        data = UtilTT.makeRecord(30000, (byte) 4);
        loc2 = physMgr.update(loc2, data, 0, data.length);
        DataInputOutput a4 = new DataInputOutput();
        physMgr.fetch(a4, loc2);
        assertTrue("check data4",
                UtilTT.checkRecord(a4.toByteArray(), 30000, (byte) 4));


        // delete the record
        physMgr.free(loc2);

        f.forceClose();
    }

    public void testTwoRecords() throws IOException {
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        PhysicalRowIdManager physmgr = new PhysicalRowIdManager(f, pm);

        physmgr.insert(new byte[1024],0,1024);
        physmgr.insert(new byte[100],0,100);

        assertEquals(listRecords(pm),arrayList(1024,100));

    }

    public void testDeleteRecord() throws IOException {
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        PhysicalRowIdManager physmgr = new PhysicalRowIdManager(f, pm);

        physmgr.insert(data,0,1024);
        long recid = physmgr.insert(data,0,100);
        physmgr.insert(data,0,700);
        physmgr.free(recid);

        assertEquals(listRecords(pm), arrayList(1024, -100, 700));

    }



    public void testTwoLargeRecord() throws IOException {
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        PhysicalRowIdManager physmgr = new PhysicalRowIdManager(f, pm);

        physmgr.insert(data,0,5000);
        physmgr.insert(data,0,5000);


        assertEquals(listRecords(pm), arrayList(5000,5000));

    }


    public void testManyLargeRecord() throws IOException {
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        PhysicalRowIdManager physmgr = new PhysicalRowIdManager(f, pm);

        physmgr.insert(data,0,5002);
        long id1 = physmgr.insert(data,0,5003);
        physmgr.insert(data,0,5005);
        long id2 = physmgr.insert(data,0,5006);
        physmgr.insert(data,0,5007);
        physmgr.insert(data,0,5008);
        physmgr.free(id1);
        physmgr.free(id2);


        assertEquals(listRecords(pm), arrayList(5002,-5003,5005,-5006,5007,5008));

    }


    public void testSplitRecordAcrossPage() throws IOException {
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        PhysicalRowIdManager physmgr = new PhysicalRowIdManager(f, pm);

        physmgr.insert(data,0,3000);
        long id = physmgr.insert(data,0,3000);
        physmgr.insert(data,0,1000);
        physmgr.free(id);

        //record which crosses page should be sliced to two, so it does not cross the page
        int firstSize = Storage.BLOCK_SIZE - Magic.DATA_PAGE_O_DATA - RecordHeader.SIZE - 3000 - RecordHeader.SIZE;
        int secondSize = 3000-firstSize - RecordHeader.SIZE;


        //TODO decide about this
        //assertEquals(listRecords(pm), arrayList(3000,-firstSize,-secondSize, 1000));

    }


    public void testFreeMidPages() throws IOException {
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        PhysicalRowIdManager physmgr = new PhysicalRowIdManager(f, pm);

        physmgr.insert(data,0,3000);
        long id = physmgr.insert(data,0,30000);
        physmgr.insert(data,0,1000);
        physmgr.free(id);

        //if record occupies multiple pages, mid pages should be freed and record trimmed.
        int newSize = 30000;

        while(newSize>Storage.BLOCK_SIZE - Magic.DATA_PAGE_O_DATA)
            newSize = newSize - (Storage.BLOCK_SIZE - Magic.DATA_PAGE_O_DATA);


        assertEquals(listRecords(pm), arrayList(3000, -newSize, 1000));

    }

    
    
    /** return list of records in pageman, negative numbers are free records*/
    List<Integer> listRecords(PageManager pageman) throws IOException {
       int pos = Magic.DATA_PAGE_O_DATA;
       List<Integer> ret =new ArrayList<Integer>();
       for(
           long pageid = pageman.getFirst(Magic.USED_PAGE);
           pageid!=0;
           pageid = pageman.getNext(pageid)){
           
           BlockIo block = pageman.file.get(pageid);


                      
           while(pos < Storage.BLOCK_SIZE-RecordHeader.SIZE){

               int size = RecordHeader.getAvailableSize(block, (short) pos);
               if(size == 0) 
                   break;
               int currSize =RecordHeader.getCurrentSize(block, (short) pos);
               pos+=size+RecordHeader.SIZE;
               if(currSize==0)
                   size = -size;
               ret.add(size);
           }
           
           pos = pos +Magic.DATA_PAGE_O_DATA - Storage.BLOCK_SIZE;

           pageman.file.release(block);
       }
                   

       return ret;
    }
    
    List<Integer> arrayList(Integer... args){
        ArrayList<Integer> ret = new ArrayList<Integer>();
        for(Integer i:args)
            ret.add(i);
        return ret;
    }

}
