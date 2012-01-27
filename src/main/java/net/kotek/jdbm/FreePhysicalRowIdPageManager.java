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

/**
 * This class manages free physical rowid pages and provides methods to free and allocate physical rowids on a high
 * level.
 */
final class FreePhysicalRowIdPageManager {
    
    /**
     * Used to place a limit on the wasted capacity resulting in a modified first fit policy for re-allocated of free
     * records. This value is the maximum first fit waste that is accepted when scanning the available slots on a given
     * page of the free physical row page list.
     */
    static final transient int wasteMargin = 128;

    /**
     * Used to place a limit on the wasted capacity resulting in a modified first fit policy for re-allocated of free
     * records. This value is the upper bound of waste that is accepted before scanning another page on the free
     * physical row page list. If no page can be found whose waste for the re-allocation request would be less than this
     * value then a new page will be allocated and the requested physical row will be allocated from that new page.
     */
    static final transient int wasteMargin2 = Magic.PAGE_HEADER_SIZE / 4;
    
    // our record file
    protected RecordFile _file;

    // our page manager
    protected PageManager _pageman;

    private final Utils.LongArrayList freeBlocksInTransactionRowid = new Utils.LongArrayList();
    private final Utils.IntArrayList freeBlocksInTransactionSize = new Utils.IntArrayList();


    /**
     * Creates a new instance using the indicated record file and page manager.
     */
    FreePhysicalRowIdPageManager(RecordFile file, PageManager pageman) throws IOException {
        _file = file;
        _pageman = pageman;
    }

    private int lastMaxSize = -1;

    /**
     * Returns a free physical rowid of the indicated size, or null if nothing was found. This scans the free physical
     * row table, which is modeled as a linked list of pages. Each page on that list has slots that are either free
     * (awaiting the insertion of the location of a free physical row) or available for reallocation requests. An
     * allocated slot is indicated by a non-zero size field in that slot and the size is the size of the available free
     * record in bytes.
     */
    long get(int size) throws IOException {


        //requested record is bigger than any previously found
        if (lastMaxSize != -1 && size > lastMaxSize)
            return 0;

        // Loop through the free physical rowid list until we get
        // a rowid that's large enough.
        long retval = 0;

        int maxSize = -1;
        for (long current = _pageman.getFirst(Magic.FREEPHYSIDS_PAGE); current != 0; current = _pageman.getNext(current)) {
            BlockIo fp = _file.get(current);
            int slot = fp.FreePhysicalRowId_getFirstLargerThan(size);
            if (slot > 0) {
                //reset maximal size, as record has changed
                lastMaxSize = -1;
                // got one!
                retval = fp.FreePhysicalRowId_slotToLocation(slot);

                fp.FreePhysicalRowId_free(slot);
                if (fp.FreePhysicalRowId_getCount() == 0) {
                    // page became empty - free it
                    _file.release(current, false);
                    _pageman.free(Magic.FREEPHYSIDS_PAGE, current);
                } else {
                    _file.release(current, true);
                }

                return retval;
            } else {
                if (maxSize < -slot)
                    maxSize = -slot;
                // no luck, go to next page
                _file.release(current, false);
            }

        }
        //update maximal size available
        lastMaxSize = maxSize;

        return 0;
    }


    /**
     * Puts the indicated rowid on the free list, which avaits for commit
     */
    void put(long rowid, int size) throws IOException {
        freeBlocksInTransactionRowid.add(rowid);
        freeBlocksInTransactionSize.add(size);

        //TODO check there is an commit on close if transactionsDisabled
    }

    public void commit() throws IOException {
        //write all uncommited free records
        int rowidpos = 0;

        if (freeBlocksInTransactionRowid.size < 200) { //if there is too much released records, just write those into new page, this greatly speedsup imports.

            //iterate over filled pages
            final boolean fromLast = Math.random() < 0.5; //iterating from begining makes pages filled wery quickly, so swap it sometimes.
            for (long current = fromLast ? _pageman.getLast(Magic.FREEPHYSIDS_PAGE) : _pageman.getFirst(Magic.FREEPHYSIDS_PAGE);
                 current != 0;
                 current = fromLast ? _pageman.getPrev(current) : _pageman.getNext(current)
                    ) {
                BlockIo fp = _file.get(current);
                int slot = fp.FreePhysicalRowId_getFirstFree();
                //iterate over free slots in page and fill them
                while (slot != -1 && rowidpos < freeBlocksInTransactionRowid.size) {
                    int size = freeBlocksInTransactionSize.data[rowidpos];
                    long rowid = freeBlocksInTransactionRowid.data[rowidpos++];

                    short freePhysRowId = fp.FreePhysicalRowId_alloc(slot);
                    fp.pageHeaderSetLocation(freePhysRowId, rowid);
                    fp.FreePhysicalRowId_setSize(freePhysRowId, size);
                    slot = fp.FreePhysicalRowId_getFirstFree();
                }
                _file.release(current, true);
                if (!(rowidpos < freeBlocksInTransactionRowid.size))
                    break;
            }
        }

        //now we propably filled all already allocated pages,
        //time to start allocationg new pages
        while (rowidpos < freeBlocksInTransactionRowid.size) {
            //allocate new page
            long freePage = _pageman.allocate(Magic.FREEPHYSIDS_PAGE);
            BlockIo fp = _file.get(freePage);
            int slot = fp.FreePhysicalRowId_getFirstFree();
            //iterate over free slots in page and fill them
            while (slot != -1 && rowidpos < freeBlocksInTransactionRowid.size) {
                int size = freeBlocksInTransactionSize.data[rowidpos];
                long rowid = freeBlocksInTransactionRowid.data[rowidpos++];
                short freePhysRowId = fp.FreePhysicalRowId_alloc(slot);
                fp.pageHeaderSetLocation(freePhysRowId, rowid);
                fp.FreePhysicalRowId_setSize(freePhysRowId, size);
                slot = fp.FreePhysicalRowId_getFirstFree();
            }
            _file.release(freePage, true);
            if (!(rowidpos < freeBlocksInTransactionRowid.size))
                break;
        }

        if (rowidpos < freeBlocksInTransactionRowid.size)
            throw new InternalError();

        freeBlocksInTransactionRowid.clear();
        freeBlocksInTransactionSize.clear();

    }

    public void rollback() {
        freeBlocksInTransactionRowid.clear();
        freeBlocksInTransactionSize.clear();
    }
}
