/*
 *  $Id: TestPageManager.java,v 1.3 2003/09/21 15:49:02 boisvert Exp $
 *
 *  Unit tests for PageManager class
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
 *  This class contains all Unit tests for {@link PageManager}.
 */
public class TestPageManager extends TestCaseWithTestFile {


    /**
     *  Test constructor
     */
    public void testCtor() throws Exception {
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);

        f.forceClose();
    }

    /**
     *  Test allocations on a single list.
     */
    public void testAllocSingleList() throws Exception {
    	String file = newTestFile();
        RecordFile f = new RecordFile(file);
        PageManager pm = new PageManager(f);
        for (int i = 0; i < 100; i++) {
            assertEquals("allocate ", (long) i + 1,
             pm.allocate(Magic.USED_PAGE));
        }
        pm.close();
        f.close();

        f = new RecordFile(file);
        pm = new PageManager(f);
        PageCursor curs = new PageCursor(pm, Magic.USED_PAGE);
        long i = 1;
        while (true) {
            long cur = curs.next();
            if (cur == 0)
            	break;
            assertEquals("next", i++, cur);
            if (i > 120)
            	fail("list structure not ok");
        }
        assertEquals("total", 101, i);
        pm.close();
        f.close();
    }

    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(new TestSuite(TestPageManager.class));
    }
}
