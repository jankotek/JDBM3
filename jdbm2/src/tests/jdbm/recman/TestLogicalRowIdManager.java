/*
 *  $Id: TestLogicalRowIdManager.java,v 1.3 2003/09/21 15:49:02 boisvert Exp $
 *
 *  Unit tests for LogicalRowIdManager class
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

import junit.framework.TestSuite;

/**
 *  This class contains all Unit tests for {@link LogicalRowIdManager}.
 */
public class TestLogicalRowIdManager extends _TestCaseWithTestFile {


    /**
     *  Test constructor
     */
    public void testCtor() throws Exception {
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        LogicalRowIdManager logMgr = new LogicalRowIdManager(f, pm);

        f.forceClose();
    }

    /**
     *  Test basics
     */
    public void testBasics() throws Exception {
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        LogicalRowIdManager logMgr = new LogicalRowIdManager(f, pm);
        Location physid = new Location(20, (short) 234);

        Location logid = logMgr.insert(physid);
        assertEquals("check one", physid, logMgr.fetch(logid));

        physid = new Location(10, (short) 567);
        logMgr.update(logid, physid);
        assertEquals("check two", physid, logMgr.fetch(logid));

        logMgr.delete(logid);

        f.forceClose();
    }


    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(new TestSuite(TestLogicalRowIdManager.class));
    }
}
