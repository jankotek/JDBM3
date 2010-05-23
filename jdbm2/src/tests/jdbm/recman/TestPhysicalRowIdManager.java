/*

 *  $Id: TestPhysicalRowIdManager.java,v 1.5 2003/09/21 15:49:02 boisvert Exp $
 *
 *  Unit tests for PhysicalRowIdManager class
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
import java.io.ByteArrayOutputStream;

/**
 *  This class contains all Unit tests for {@link PhysicalRowIdManager}.
 */
public class TestPhysicalRowIdManager extends _TestCaseWithTestFile {




    /**
     *  Test constructor
     */
    public void testCtor() throws Exception {
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        PhysicalRowIdManager physMgr = new PhysicalRowIdManager(f, pm);

        f.forceClose();
    }

    /**
     *  Test basics
     */
    public void testBasics() throws Exception {

        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        PhysicalRowIdManager physMgr = new PhysicalRowIdManager(f, pm);

        // insert a 10,000 byte record.
        byte[] data = _TestUtil.makeRecord(10000, (byte) 1);
        Location loc = physMgr.insert( data, 0, data.length );
		ByteArrayOutputStream a1 = new ByteArrayOutputStream();
		physMgr.fetch(a1,loc);
        assertTrue("check data1",
               _TestUtil.checkRecord(a1.toByteArray(), 10000, (byte) 1));

        // update it as a 20,000 byte record.
        data = _TestUtil.makeRecord(20000, (byte) 2);
        Location loc2 = physMgr.update(loc, data, 0, data.length );
		ByteArrayOutputStream a2 = new ByteArrayOutputStream();
		physMgr.fetch(a2,loc2);
        assertTrue("check data2",
               _TestUtil.checkRecord(a2.toByteArray(), 20000, (byte) 2));

        // insert a third record. This'll effectively block the first one
        // from growing
        data = _TestUtil.makeRecord(20, (byte) 3);
        Location loc3 = physMgr.insert(data, 0, data.length );
		ByteArrayOutputStream a3 = new ByteArrayOutputStream();
		physMgr.fetch(a3,loc3);
        assertTrue("check data3",
               _TestUtil.checkRecord(a3.toByteArray(), 20, (byte) 3));

        // now, grow the first record again
        data = _TestUtil.makeRecord(30000, (byte) 4);
        loc2 = physMgr.update(loc2, data, 0, data.length );
        ByteArrayOutputStream a4 = new ByteArrayOutputStream();
        physMgr.fetch(a4,loc2);
        assertTrue("check data4",
               _TestUtil.checkRecord(a4.toByteArray(), 30000, (byte) 4));


        // delete the record
        physMgr.delete(loc2);

        f.forceClose();
    }
}
