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

package jdbm.recman;
import java.io.ByteArrayOutputStream;

/**
 *  This class contains all Unit tests for {@link PhysicalRowIdManager}.
 */
public class TestPhysicalRowIdManager extends TestCaseWithTestFile {




    /**
     *  Test constructor
     */
    public void testCtor() throws Exception {
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        RecordFile free = newRecordFile();
        PageManager pmfree = new PageManager(free);

        PhysicalRowIdManager physMgr = new PhysicalRowIdManager(f, pm,new FreePhysicalRowIdPageManager(free, pmfree,false));

        f.forceClose();
    }

    /**
     *  Test basics
     */
    public void testBasics() throws Exception {

        RecordFile f = newRecordFile();        
        PageManager pm = new PageManager(f);
        RecordFile free = newRecordFile();
        PageManager pmfree = new PageManager(free);
        PhysicalRowIdManager physMgr = new PhysicalRowIdManager(f, pm, new FreePhysicalRowIdPageManager(free, pmfree,false));

        // insert a 10,000 byte record.
        byte[] data = UtilTT.makeRecord(10000, (byte) 1);
        long loc = physMgr.insert( data, 0, data.length );
		ByteArrayOutputStream a1 = new ByteArrayOutputStream();
		physMgr.fetch(a1,loc);
        assertTrue("check data1",
               UtilTT.checkRecord(a1.toByteArray(), 10000, (byte) 1));

        // update it as a 20,000 byte record.
        data = UtilTT.makeRecord(20000, (byte) 2);
        long loc2 = physMgr.update(loc, data, 0, data.length );
		ByteArrayOutputStream a2 = new ByteArrayOutputStream();
		physMgr.fetch(a2,loc2);
        assertTrue("check data2",
               UtilTT.checkRecord(a2.toByteArray(), 20000, (byte) 2));

        // insert a third record. This'll effectively block the first one
        // from growing
        data = UtilTT.makeRecord(20, (byte) 3);
        long loc3 = physMgr.insert(data, 0, data.length );
		ByteArrayOutputStream a3 = new ByteArrayOutputStream();
		physMgr.fetch(a3,loc3);
        assertTrue("check data3",
               UtilTT.checkRecord(a3.toByteArray(), 20, (byte) 3));

        // now, grow the first record again
        data = UtilTT.makeRecord(30000, (byte) 4);
        loc2 = physMgr.update(loc2, data, 0, data.length );
        ByteArrayOutputStream a4 = new ByteArrayOutputStream();
        physMgr.fetch(a4,loc2);
        assertTrue("check data4",
               UtilTT.checkRecord(a4.toByteArray(), 30000, (byte) 4));


        // delete the record
        physMgr.delete(loc2);

        f.forceClose();
    }
    
}
