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

public class FreePhysicalRowIdPageTest extends TestCase {


    /**
     * Test basics
     */
    public void testBasics() throws Exception {
        byte[] data = new byte[Storage.BLOCK_SIZE];
        BlockIo page = new BlockIo(0, data);


        // we have a completely empty page.
        assertEquals("zero count", 0, page.FreePhysicalRowId_getCount());

        // three allocs
        short id = page.FreePhysicalRowId_alloc(0);
        id = page.FreePhysicalRowId_alloc(1);
        id = page.FreePhysicalRowId_alloc(2);
        assertEquals("three count", 3, page.FreePhysicalRowId_getCount());

        // setup last id (2)
        page.pageHeaderSetLocation(id, Location.toLong(1, (short) 2));
        page.FreePhysicalRowId_setSize(id, 3);

        // two frees
        page.FreePhysicalRowId_free(0);
        page.FreePhysicalRowId_free(1);
        assertEquals("one left count", 1, page.FreePhysicalRowId_getCount());
        assertTrue("isfree 0", page.FreePhysicalRowId_isFree(0));
        assertTrue("isfree 1", page.FreePhysicalRowId_isFree(1));
        assertTrue("isalloc 2", !page.FreePhysicalRowId_isFree(2));

        // now, create a new page over the data and check whether
        // it's all the same.
        page = new BlockIo(0,data);

        assertEquals("2: one left count", 1, page.FreePhysicalRowId_getCount());
        assertTrue("2: isfree 0", page.FreePhysicalRowId_isFree(0));
        assertTrue("2: isfree 1", page.FreePhysicalRowId_isFree(1));
        assertTrue("2: isalloc 2", !page.FreePhysicalRowId_isFree(2));

        id = page.FreePhysicalRowId_slotToOffset(2);
        long loc = page.pageHeaderGetLocation(id);
        assertEquals("block", 1, Location.getBlock(loc));
        assertEquals("offset", 2, Location.getOffset(loc));
        assertEquals("size", 3, page.FreePhysicalRowId_getSize(id));

    }


    public void testOffsetSlotConversion() {
        byte[] data = new byte[Storage.BLOCK_SIZE];
        BlockIo page = new BlockIo(0, data);

        for (int slot = 0; slot < 1e5; slot++) {
            short pos = page.FreePhysicalRowId_slotToOffset(slot);
            if (pos > 20000) return; //out of page size
            int slot2 = page.FreePhysicalRowId_offsetToSlot(pos);
            assertEquals("failed for " + slot + " , " + pos, slot, slot2);
        }
    }

}
