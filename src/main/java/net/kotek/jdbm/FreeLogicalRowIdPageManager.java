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

import java.io.IOException;
import java.util.Arrays;

/**
 * This class manages free Logical rowid pages and provides methods
 * to free and allocate Logical rowids on a high level.
 */
final class FreeLogicalRowIdPageManager {
    // our record file
    private RecordFile file;
    // our page manager
    private PageManager pageman;

    private long[] inTransRowid = new long[4];
    private int inTransSize = 0;


    /**
     * Creates a new instance using the indicated record file and
     * page manager.
     */
    FreeLogicalRowIdPageManager(RecordFile file,
                                PageManager pageman) throws IOException {
        this.file = file;
        this.pageman = pageman;
    }

    /**
     * Returns a free Logical rowid, or
     * 0 if nothing was found.
     */
    long get() throws IOException {
        if (inTransSize != 0) {
            return inTransRowid[--inTransSize];
        }

        // Loop through the free Logical rowid list until we get
        // the first rowid.
        long retval = 0;
        for (long current = pageman.getFirst(Magic.FREELOGIDS_PAGE); current != 0; current = pageman.getNext(current)) {
            BlockIo fp = file.get(current);
            short slot = fp.FreeLogicalRowId_getFirstAllocated();
            if (slot != -1) {
                // got one!
                retval = fp.FreeLogicalRowId_slotToLocation(slot);

                fp.FreeLogicalRowId_free(slot);
                if (fp.FreeLogicalRowId_getCount() == 0) {
                    // page became empty - free it
                    file.release(current, false);
                    pageman.free(Magic.FREELOGIDS_PAGE, current);
                } else
                    file.release(current, true);

                return retval;
            } else {
                // no luck, go to next page
                file.release(current, false);
            }
        }
        return 0;
    }

    /**
     * Puts the indicated rowid on the free list
     */
    void put(long rowid) throws IOException {
        //ensure capacity
        if(inTransSize==inTransRowid.length)
            inTransRowid = Arrays.copyOf(inTransRowid,inTransRowid.length*2);
        //add record and increase size
        inTransRowid[inTransSize]=rowid;
        inTransSize++;
    }


    public void commit() throws IOException {
        //write all uncommited free records
        int rowIdPos = 0;

        //iterate over filled pages
        for (long current = pageman.getFirst(Magic.FREELOGIDS_PAGE); current != 0; current = pageman.getNext(current)) {

            final BlockIo fp = file.get(current);
            short slot = fp.FreeLogicalRowId_getFirstFree();
            //iterate over free slots in page and fill them
            while (slot != -1 && rowIdPos < inTransSize) {
                final long rowid = inTransRowid[rowIdPos++];
                final short freePhysRowId = fp.FreeLogicalRowId_alloc(slot);
                fp.pageHeaderSetLocation(freePhysRowId, rowid);
                slot = fp.FreeLogicalRowId_getFirstFree();
            }
            file.release(current, true);
            if (!(rowIdPos < inTransSize))
                break;
        }

        //now we propably filled all already allocated pages,
        //time to start allocationg new pages
        while (rowIdPos < inTransSize) {
            //allocate new page
            long freePage = pageman.allocate(Magic.FREELOGIDS_PAGE);
            BlockIo fp = file.get(freePage);
            short slot = fp.FreeLogicalRowId_getFirstFree();
            //iterate over free slots in page and fill them
            while (slot != -1 && rowIdPos < inTransSize) {
                long rowid = inTransRowid[rowIdPos++];
                short freePhysRowId = fp.FreeLogicalRowId_alloc(slot);
                fp.pageHeaderSetLocation(freePhysRowId, rowid);
                slot = fp.FreeLogicalRowId_getFirstFree();
            }
            file.release(freePage, true);
            if (!(rowIdPos < inTransSize))
                break;
        }

        if (rowIdPos < inTransSize)
            throw new InternalError();

        //rollback is called just to clear the list, we do not really want rolling back
        rollback();

    }
    
    public void rollback(){
        if(inTransRowid.length>128)
            inTransRowid = new long[4];
        inTransSize = 0;
    }
}
