/*
 *  $Id: TestFreeLogicalRowIdPageManager.java,v 1.4 2003/09/21 15:49:02 boisvert Exp $
 *
 *  Unit tests for FreeLogicalRowIdPageManager class
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
 * This class contains all Unit tests for {@link FreeLogicalRowIdPageManager}.
 */
public class TestFreeLogicalRowIdPageManager extends TestCaseWithTestFile {


	/**
	 * Test constructor
	 */
	public void testCtor() throws Exception {
		RecordFile f = newRecordFile();
		PageManager pm = new PageManager(f);
		FreeLogicalRowIdPageManager freeMgr = new FreeLogicalRowIdPageManager(
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
		FreeLogicalRowIdPageManager freeMgr = new FreeLogicalRowIdPageManager(
				f, pm);

		// allocate a rowid - should fail on an empty file
		Location loc = freeMgr.get();
		assertTrue("loc is not null?", loc == null);

		pm.close();
		f.close();
	}

	/**
	 * Runs all tests in this class
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(new TestSuite(
				TestFreeLogicalRowIdPageManager.class));
	}
}
