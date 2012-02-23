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
 * This class manages free physical rowid pages and provides methods to free and allocate physical rowids on a high
 * level.
 */
final class FreePhysicalRowIdPageManager {

    /**
     * massive deletes leaves lot of free phys pages. 
     * This slows down new allocations, so this is one of the criteria for 
     * autodefragmentation 
     */
    static final int DEFRAGMENT_AFTER_N_PAGES = 255;

    boolean needsDefragementation = false;

    
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

    private long[] inTransRecid = new long[4];
    private int[] inTransCapacity = new int[4];
    private int inTransSize = 0;

    /**
     * Creates a new instance using the indicated record file and page manager.
     */
    FreePhysicalRowIdPageManager(RecordFile file, PageManager pageman) throws IOException {
        _file = file;
        _pageman = pageman;
    }

    private int lastMaxSize = -1;

    /**
     * Returns first slot with available size >= indicated size, or minus maximal size available on this page
     *
     * @param b  BlockIO
     * @param requestedSize requested allocation size.
     */
    static int getFirstLargerThan(BlockIo b, final int requestedSize) {

        int maxSize = 0;
        /*
           * Tracks slot of the smallest available physical row on the page.
           */
        int bestSlot = -1;
        /*
           * Tracks size of the smallest available physical row on the page.
           */
        int bestSlotSize = 0;
        /*
           * Scan each slot in the page.
           */
        for (int i = 0; i < Magic.FreePhysicalRowId_ELEMS_PER_PAGE; i++) {
            /*
                * When large allocations are used, the space wasted by the first fit policy can become very large (25% of
                * the store). The first fit policy has been modified to only accept a record with a maximum amount of
                * wasted capacity given the requested allocation size.
                */
            // Note: isAllocated(i) is equiv to get(i).getSize() != 0
            //long theSize = get(i).getSize(); // capacity of this free record.
            short pos = b.FreePhysicalRowId_slotToOffset(i);
            int currentRecSize = b.FreePhysicalRowId_getSize(pos); // capacity of this free record.
            if (currentRecSize > maxSize) maxSize = currentRecSize;
            int waste = currentRecSize - requestedSize; // when non-negative, record has suf. capacity.
            if (waste >= 0) {
                if (waste < wasteMargin) {
                    return i; // record has suf. capacity and not too much waste.
                } else if (bestSlotSize >= currentRecSize) {
                    /*
                          * This slot is a better fit that any that we have seen so far on this page so we update the slot#
                          * and available size for that slot.
                          */
                    bestSlot = i;
                    bestSlotSize = currentRecSize;
                }
            }
        }
        if (bestSlot != -1) {
            /*
                * An available slot was identified that is large enough, but it exceeds the first wasted capacity limit. At
                * this point we check to see whether it is under our second wasted capacity limit. If it is, then we return
                * that slot.
                */
            long waste = bestSlotSize - requestedSize; // when non-negative, record has suf. capacity.
            if (waste >= 0 && waste < wasteMargin2) {
                // record has suf. capacity and not too much waste.
                return bestSlot;
            }
            /*
                * Will scan next page on the free physical row page list.
                */
        }

        return -maxSize;
    }

    /**
     * Returns a free physical rowid of the indicated size, or null if nothing was found. This scans the free physical
     * row table, which is modeled as a linked list of pages. Each page on that list has slots that are either free
     * (awaiting the insertion of the location of a free physical row) or available for reallocation requests. An
     * allocated slot is indicated by a non-zero size field in that slot and the size is the size of the available free
     * record in bytes.
     */
    long get(final int size) throws IOException {
        //first check data in transaction, maybe some of them are usable

        for(int i = 0;i<inTransSize;i++){
            //check if size is in limits
            if(inTransCapacity[i]>=size &&  inTransCapacity[i]<size + wasteMargin)
            {
                final long ret = inTransRecid[i];
                //move last item to current pos and delete current item
                inTransSize--;
                inTransRecid[i] = inTransRecid[inTransSize];
                inTransCapacity[i] = inTransCapacity[inTransSize];

                return ret;
            }
                
        }

        int pageCounter = 0;

        //requested record is bigger than any previously found
        if (lastMaxSize != -1 && size > lastMaxSize)
            return 0;

        // Loop through the free physical rowid list until we get
        // a rowid that's large enough.
        long retval = 0;

        int maxSize = -1;
        for (long current = _pageman.getFirst(Magic.FREEPHYSIDS_PAGE); current != 0; current = _pageman.getNext(current)) {
            BlockIo fp = _file.get(current);
            pageCounter++;
            int slot = getFirstLargerThan(fp, size);
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



                if(pageCounter>DEFRAGMENT_AFTER_N_PAGES)
                    needsDefragementation = true;

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


        if(pageCounter>DEFRAGMENT_AFTER_N_PAGES)
            needsDefragementation = true;

        return 0;
    }


    /**
     * Puts the indicated rowid on the free list, which avaits for commit
     */
    void put(final long rowid, final int size) throws IOException {
        //ensure capacity
        if(inTransSize==inTransRecid.length){
            inTransRecid = Arrays.copyOf(inTransRecid, inTransRecid.length * 2);
            inTransCapacity = Arrays.copyOf(inTransCapacity, inTransCapacity.length * 2);
        }
        inTransRecid[inTransSize] = rowid;
        inTransCapacity[inTransSize] = size;
        inTransSize++;
    }

    public void commit() throws IOException {
        if(inTransSize==0)
            return;

        quickSort(inTransRecid,inTransCapacity,0,inTransSize-1);

        //try to merge records released next to each other into single one
        int prevIndex = 0;
        for(int i=1;i<inTransSize;i++){
            if(inTransCapacity[i] == 0) continue;

            if(inTransCapacity[i] + inTransCapacity[prevIndex]<Short.MAX_VALUE &&
                inTransRecid[prevIndex] + inTransCapacity[i] == inTransRecid[i]){
                //increase previous record size and effectively delete old record size
                final long blockId = Location.getBlock(inTransRecid[prevIndex]);
                BlockIo b = _file.get(blockId);
                RecordHeader.setCurrentSize(b,Location.getOffset(inTransRecid[prevIndex]), 0);
                inTransCapacity[prevIndex]+=inTransCapacity[i];
                RecordHeader.setAvailableSize(b,Location.getOffset(inTransRecid[prevIndex]), inTransCapacity[prevIndex]);
                _file.release(b);
                //zero out curr record, so it does not get added to list
                inTransRecid[i] = 0;
                inTransCapacity[i] = 0;
                //move to next, without leaving previous record
                i++;
                continue;
            }
            prevIndex = i;
        }

        //write all uncommited free records
        int rowidpos = 0;


        //iterate over free recid pages

        long curpage = _pageman.getFirst(Magic.FREEPHYSIDS_PAGE);


        if(_file.transactionsDisabled && inTransSize>200){
            //iterating over existing free pages can be time consuming
            //so disable it under some conditions and start allocating new freePhys pages
            //straight away
            curpage = 0;
        }

        while (rowidpos < inTransSize) {

            //get next page for free recid, allocate new page if needed
            BlockIo fp = curpage!=0 ? _file.get(curpage) :
                    _file.get(_pageman.allocate(Magic.FREEPHYSIDS_PAGE));
            
            int slot = fp.FreePhysicalRowId_getFirstFree();
            //iterate over free slots in page and fill them
            while (slot != -1 && rowidpos < inTransSize) {
                int size = inTransCapacity[rowidpos];
                long rowid = inTransRecid[rowidpos++];
                if(size == 0) continue;

                short freePhysPos = fp.FreePhysicalRowId_alloc(slot);
                fp.pageHeaderSetLocation(freePhysPos, rowid);
                fp.FreePhysicalRowId_setSize(freePhysPos, size);
                slot = fp.FreePhysicalRowId_getFirstFree();


            }
            _file.release(fp);
            if (!(rowidpos < inTransSize))
                break;


            if(curpage!=0){
                    curpage = _pageman.getNext(curpage);
            }
        }

        //rollback is called just to clear the list, we do not really want rolling back
        rollback();
    }

    /** quick sort which also sorts elements in second array*/
    private static void quickSort(final long array[], final int array2[],final int low, final int n){
        long temp;
        int temp2;
        int lo = low;
        int hi = n;
        if (lo >= n) {
            return;
        }
        long mid = array[(lo + hi) / 2];
        while (lo < hi) {
            while (lo<hi && array[lo] < mid) {
                lo++;
            }
            while (lo<hi && array[hi] > mid) {
                hi--;
            }
            if (lo < hi) {
                temp = array[lo];
                array[lo] = array[hi];
                array[hi] = temp;
                temp2 = array2[lo];
                array2[lo] = array2[hi];
                array2[hi] = temp2;
            }
        }
        if (hi < lo) {
            temp2 = hi;
            hi = lo;
            lo = temp2;
        }
        quickSort(array, array2, low, lo);
        quickSort(array, array2, lo == low ? lo+1 : lo, n);
    }




    public void rollback() {
        if(inTransRecid.length>128)
            inTransRecid = new long[4];
        if(inTransCapacity.length>128)
            inTransCapacity = new int[4];
        inTransSize = 0;
    }
}
