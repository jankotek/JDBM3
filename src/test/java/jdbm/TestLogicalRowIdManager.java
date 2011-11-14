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

import junit.framework.TestSuite;

/**
 *  This class contains all Unit tests for {@link LogicalRowIdManager}.
 */
public class TestLogicalRowIdManager extends TestCaseWithTestFile {


    /**
     *  Test constructor
     */
    public void testCtor() throws Exception {
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        RecordFile free = newRecordFile();
        PageManager pmfree = new PageManager(free);
        
        LogicalRowIdManager logMgr = new LogicalRowIdManager(f, pm, new FreeLogicalRowIdPageManager(free, pmfree));

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
        LogicalRowIdManager logMgr = new LogicalRowIdManager(f, pm, new FreeLogicalRowIdPageManager(free, pmfree));
        long physid = Location.toLong(20, (short) 234);

        long logid = logMgr.insert(physid);
        assertEquals("check one", physid, logMgr.fetch(logid));

        physid = Location.toLong(10, (short) 567);
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
