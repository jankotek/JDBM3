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

import junit.framework.TestCase;

public class FreeLogicalRowIdPageTest extends TestCase {



    /**
     * Test basics
     */
    public void testBasics() throws Exception {
        byte[] data = new byte[Storage.BLOCK_SIZE];
        BlockIo page = new BlockIo(0, data);

        // we have a completely empty page.
        assertEquals("zero count", 0, page.FreeLogicalRowId_getCount());

        // three allocs
        short id = page.FreeLogicalRowId_alloc((short) 0);
        id = page.FreeLogicalRowId_alloc((short) 1);
        id = page.FreeLogicalRowId_alloc((short) 2);
        assertEquals("three count", 3, page.FreeLogicalRowId_getCount());

        // setup last id (2)
        page.pageHeaderSetLocation(id, Location.toLong(1, (short) 2));


        // two frees
        page.FreeLogicalRowId_free((short) 0);
        page.FreeLogicalRowId_free((short) 1);
        assertEquals("one left count", 1, page.FreeLogicalRowId_getCount());
        assertTrue("isfree 0", page.FreeLogicalRowId_isFree(0));
        assertTrue("isfree 1", page.FreeLogicalRowId_isFree(1));
        assertTrue("isalloc 2", page.FreeLogicalRowId_isAllocated(2));

        // now, create a new page over the data and check whether
        // it's all the same.
        page = new BlockIo(0,data);

        assertEquals("2: one left count", 1, page.FreeLogicalRowId_getCount());
        assertTrue("2: isfree 0", page.FreeLogicalRowId_isFree(0));
        assertTrue("2: isfree 1", page.FreeLogicalRowId_isFree(1));
        assertTrue("2: isalloc 2", page.FreeLogicalRowId_isAllocated(2));

        id = page.FreeLogicalRowId_slotToOffset(2);
        long loc = page.pageHeaderGetLocation(id);
        assertEquals("block", 1, Location.getBlock(loc));
        assertEquals("offset", 2, Location.getOffset(loc));

    }


}
