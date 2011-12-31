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

import java.io.IOException;

/**
 * This class manages free Logical rowid pages and provides methods
 * to free and allocate Logical rowids on a high level.
 */
final class FreeLogicalRowIdPageManager {
    // our record file
    private RecordFile file;
    // our page manager
    private PageManager pageman;
    private int blockSize;

    private final Utils.LongArrayList freeBlocksInTransactionRowid = new Utils.LongArrayList();


    /**
     * Creates a new instance using the indicated record file and
     * page manager.
     */
    FreeLogicalRowIdPageManager(RecordFile file,
                                PageManager pageman) throws IOException {
        this.file = file;
        this.pageman = pageman;
        this.blockSize = Storage.BLOCK_SIZE;
    }

    /**
     * Returns a free Logical rowid, or
     * 0 if nothing was found.
     */
    long get() throws IOException {
        if (freeBlocksInTransactionRowid.size != 0) {
            long first = freeBlocksInTransactionRowid.data[freeBlocksInTransactionRowid.size - 1];
            freeBlocksInTransactionRowid.removeLast();
            return first;
        }

        // Loop through the free Logical rowid list until we get
        // the first rowid.
        long retval = 0;
        for (long current = pageman.getFirst(Magic.FREELOGIDS_PAGE); current != 0; current = pageman.getNext(current)) {
            FreeLogicalRowIdPage fp = FreeLogicalRowIdPage
                    .getFreeLogicalRowIdPageView(file.get(current), blockSize);
            int slot = fp.getFirstAllocated();
            if (slot != -1) {
                // got one!
                retval = fp.slotToLocation(slot);

                fp.free(slot);
                if (fp.getCount() == 0) {
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
        freeBlocksInTransactionRowid.add(rowid);
    }


    public void commit() throws IOException {
        //write all uncommited free records
        int rowIdPos = 0;

        //iterate over filled pages
        for (long current = pageman.getFirst(Magic.FREELOGIDS_PAGE); current != 0; current = pageman.getNext(current)) {

            BlockIo curBlock = file.get(current);
            FreeLogicalRowIdPage fp = FreeLogicalRowIdPage.getFreeLogicalRowIdPageView(curBlock, blockSize);
            int slot = fp.getFirstFree();
            //iterate over free slots in page and fill them
            while (slot != -1 && rowIdPos < freeBlocksInTransactionRowid.size) {
                long rowid = freeBlocksInTransactionRowid.data[rowIdPos++];
                short freePhysRowId = fp.alloc(slot);
                fp.setLocationBlock(freePhysRowId, Location.getBlock(rowid));
                fp.setLocationOffset(freePhysRowId, Location.getOffset(rowid));
                slot = fp.getFirstFree();
            }
            file.release(current, true);
            if (!(rowIdPos < freeBlocksInTransactionRowid.size))
                break;
        }

        //now we propably filled all already allocated pages,
        //time to start allocationg new pages
        while (rowIdPos < freeBlocksInTransactionRowid.size) {
            //allocate new page
            long freePage = pageman.allocate(Magic.FREELOGIDS_PAGE);
            BlockIo curBlock = file.get(freePage);
            FreeLogicalRowIdPage fp = FreeLogicalRowIdPage.getFreeLogicalRowIdPageView(curBlock, blockSize);
            int slot = fp.getFirstFree();
            //iterate over free slots in page and fill them
            while (slot != -1 && rowIdPos < freeBlocksInTransactionRowid.size) {
                long rowid = freeBlocksInTransactionRowid.data[rowIdPos++];
                short freePhysRowId = fp.alloc(slot);
                fp.setLocationBlock(freePhysRowId, Location.getBlock(rowid));
                fp.setLocationOffset(freePhysRowId, Location.getOffset(rowid));
                slot = fp.getFirstFree();
            }
            file.release(freePage, true);
            if (!(rowIdPos < freeBlocksInTransactionRowid.size))
                break;
        }

        if (rowIdPos < freeBlocksInTransactionRowid.size)
            throw new InternalError();

        freeBlocksInTransactionRowid.clear();

    }
}
