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

/**
 * This class contains all Unit tests for {@link PhysicalFreeRowIdManager}.
 */
public class PhysicalFreeRowIdManagerTest extends TestCaseWithTestFile {

    /**
     * Test constructor
     */
    public void testCtor() throws Exception {
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        PhysicalFreeRowIdManager freeMgr = new PhysicalFreeRowIdManager(
                f, pm);

        pm.close();
        f.close();
    }

    /**
     * Test basics
     */
    public void testBasics() throws Exception {

        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        PhysicalFreeRowIdManager freeMgr = new PhysicalFreeRowIdManager(f, pm);

        // allocate 10,000 bytes - should fail on an empty file.
        long loc = freeMgr.getFreeRecord(10000);
        assertTrue("loc is not null?", loc == 0);

        pm.close();
        f.close();
    }

    public void testPhysRecRootPage() throws IOException {
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);

        long pageid = pm.allocate(Magic.FREEPHYSIDS_ROOT_PAGE);
        BlockIo p = f.get(pageid);
        p.writeInt(100,100);
        f.release(p);
        pm.commit();
        f.commit();

        p = f.get(pageid);
        assertEquals(p.readInt(100),100);

    }

    public void test_size_to_root_offset(){
        for(int i = 1;i<PhysicalFreeRowIdManager.MAX_REC_SIZE;i++){
            int offset = PhysicalFreeRowIdManager.sizeToRootOffset(i);

            assertTrue(offset<=Storage.BLOCK_SIZE);
        }
    }


    public void test_record_reallocation() throws IOException {
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        PhysicalFreeRowIdManager freeMgr = new PhysicalFreeRowIdManager(f, pm);

        freeMgr.putFreeRecord(1000,100);
        freeMgr.commit();

        assertEquals(1000,freeMgr.getFreeRecord(100-PhysicalFreeRowIdManager.ROOT_SLOT_SIZE));
        assertEquals(0,freeMgr.getFreeRecord(100-PhysicalFreeRowIdManager.ROOT_SLOT_SIZE));

    }


    public void test_all_sizes_deallocation() throws IOException {
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        PhysicalFreeRowIdManager freeMgr = new PhysicalFreeRowIdManager(f, pm);

        for(int i = 1; i<PhysicalFreeRowIdManager.MAX_REC_SIZE; i++){


            freeMgr.putFreeRecord(1000,i);
            freeMgr.commit();

            assertEquals(1000,freeMgr.getFreeRecord(i-PhysicalFreeRowIdManager.ROOT_SLOT_SIZE));
            assertEquals(0,freeMgr.getFreeRecord(i-PhysicalFreeRowIdManager.ROOT_SLOT_SIZE));
        }
        pm.close();
        f.close();

    }

}

