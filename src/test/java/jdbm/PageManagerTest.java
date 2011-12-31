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
 * This class contains all Unit tests for {@link PageManager}.
 */
public class PageManagerTest extends TestCaseWithTestFile {


    /**
     * Test constructor
     */
    public void testCtor() throws Exception {
        RecordFile f = newRecordFile();
        PageManager pm = new PageManager(f);

        f.forceClose();
    }

    /**
     * Test allocations on a single list.
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

        long i = 1;
        for (long cur = pm.getFirst(Magic.USED_PAGE); cur != 0; cur = pm.getNext(cur)) {
            assertEquals("next", i++, cur);
            if (i > 120)
                fail("list structure not ok");
        }
        assertEquals("total", 101, i);
        pm.close();
        f.close();
    }

}
