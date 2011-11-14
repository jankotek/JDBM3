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
 *  This class contains all Unit tests for {@link PageCursor}.
 */
public class TestPageCursor extends TestCaseWithTestFile {


    /**
     *  Test constructor
     */
    public void testCtor() throws Exception {
        System.out.println("TestPageCursor.testCtor");
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);
        PageCursor curs = new PageCursor(pm, 0);

        f.forceClose();
    }

    /**
     *  Test basics
     */
    public void testBasics() throws Exception {
        System.out.println("TestPageCursor.testBasics");
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);

        // add a bunch of pages
        long[] recids = new long[10];
        for (int i = 0; i < 10; i++) {
            recids[i] = pm.allocate(Magic.USED_PAGE);
        }

        PageCursor curs = new PageCursor(pm, Magic.USED_PAGE);
        for (int i = 0; i < 10; i++) {
            assertEquals("record " + i, recids[i],
             curs.next());
        }

        f.forceClose();
    }


}
