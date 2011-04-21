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

import junit.framework.TestSuite;

/**
 * This class contains all Unit tests for {@link FreePhysicalRowIdPageManager}.
 */
public class TestFreePhysicalRowIdPageManager extends TestCaseWithTestFile {

	/**
	 * Test constructor
	 */
	public void testCtor() throws Exception {
		RecordFile f = newRecordFile();
		PageManager pm = new PageManager(f);
		FreePhysicalRowIdPageManager freeMgr = new FreePhysicalRowIdPageManager(
				f, pm,false);

		pm.close();
		f.close();
	}

	/**
	 * Test basics
	 */
	public void testBasics() throws Exception {
		RecordFile f = newRecordFile();
		PageManager pm = new PageManager(f);
		FreePhysicalRowIdPageManager freeMgr = new FreePhysicalRowIdPageManager(
				f, pm,false);

		// allocate 10,000 bytes - should fail on an empty file.
		long loc = freeMgr.get(10000);
		assertTrue("loc is not null?", loc == 0);

		pm.close();
		f.close();
	}

	/**
	 * Runs all tests in this class
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(new TestSuite(
				TestFreePhysicalRowIdPageManager.class));
	}
}
