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

package org.apache.jdbm;

import java.io.IOException;
import java.util.Arrays;

/**
 * This class manages the linked lists of logical rowid pages.
 */
final class LogicalRowIdManager {
    // our record file and associated page manager
    private final PageFile file;
    private final PageManager pageman;
    static final short ELEMS_PER_PAGE = (short) ((Storage.PAGE_SIZE - Magic.PAGE_HEADER_SIZE) / Magic.PhysicalRowId_SIZE);

    private long[] freeRecordsInTransRowid = new long[4];
    private int freeRecordsInTransSize = 0;


    /** number of free logical rowids on logical free page, is SHORT*/
    static final int OFFSET_FREE_COUNT = Magic.PAGE_HEADER_SIZE;
    static final int FREE_HEADER_SIZE = Magic.PAGE_HEADER_SIZE + Magic.SZ_SHORT;
    /** maximal number of free logical per page */
    static final int FREE_RECORDS_PER_PAGE = (Storage.PAGE_SIZE -FREE_HEADER_SIZE)/6;


    /**
     * Creates a log rowid manager using the indicated record file and page manager
     */
    LogicalRowIdManager(PageFile file, PageManager pageman) throws IOException {
        this.file = file;
        this.pageman = pageman;
    }

    /**
     * Creates a new logical rowid pointing to the indicated physical id
     *
     * @param physloc physical location to point to
     * @return logical recid
     */
    long insert(final long physloc) throws IOException {
        // check whether there's a free rowid to reuse
        long retval = getFreeSlot();
        if (retval == 0) {
            // no. This means that we bootstrap things by allocating
            // a new translation page and freeing all the rowids on it.
            long firstPage = pageman.allocate(Magic.TRANSLATION_PAGE);
            short curOffset = Magic.PAGE_HEADER_SIZE;
            for (int i = 0; i < ELEMS_PER_PAGE; i++) {
                putFreeSlot(((-firstPage) << Storage.PAGE_SIZE_SHIFT) + (long) curOffset);

                curOffset += Magic.PhysicalRowId_SIZE;
            }

            retval = getFreeSlot();
            if (retval == 0) {
                throw new Error("couldn't obtain free translation");
            }
        }
        // write the translation.
        update(retval, physloc);
        return retval;
    }

    /**
     * Insert at forced location, use only for defragmentation !!
     *
     * @param logicalRowId
     * @param physLoc
     * @throws IOException
     */
    void forceInsert(final long logicalRowId, final long physLoc) throws IOException {
        if (fetch(logicalRowId) != 0)
            throw new Error("can not forceInsert, record already exists: " + logicalRowId);

        update(logicalRowId, physLoc);
    }


    /**
     * Releases the indicated logical rowid.
     */
    void delete(final long logicalrowid) throws IOException {
        //zero out old location, is needed for defragmentation
        final long pageId = -(logicalrowid>>> Storage.PAGE_SIZE_SHIFT);
        final PageIo xlatPage = file.get(pageId);
        xlatPage.pageHeaderSetLocation((short) (logicalrowid & Storage.OFFSET_MASK), 0);
        file.release(pageId, true);
        putFreeSlot(logicalrowid);
    }

    /**
     * Updates the mapping
     *
     * @param logicalrowid The logical rowid
     * @param physloc   The physical rowid
     */
    void update(final long logicalrowid, final long physloc) throws IOException {

        final long pageId =  -(logicalrowid>>> Storage.PAGE_SIZE_SHIFT);
        final PageIo xlatPage = file.get(pageId);
        xlatPage.pageHeaderSetLocation((short) (logicalrowid & Storage.OFFSET_MASK), physloc);
        file.release(pageId, true);
    }

    /**
     * Returns a mapping
     *
     * @param logicalrowid The logical rowid
     * @return The physical rowid, 0 if does not exist
     */
    long fetch(long logicalrowid) throws IOException {
        final long pageId = -(logicalrowid>>> Storage.PAGE_SIZE_SHIFT);
        final long last = pageman.getLast(Magic.TRANSLATION_PAGE);
        if (last - 1 > pageId)
            return 0;

        final short offset = (short) (logicalrowid & Storage.OFFSET_MASK);

        final PageIo xlatPage = file.get(pageId);
        final long ret =  xlatPage.pageHeaderGetLocation(offset);


        file.release(pageId, false);
        return ret;
    }

    void commit() throws IOException {
        if(freeRecordsInTransSize==0) return;

        long freeRecPageId = pageman.getLast(Magic.FREELOGIDS_PAGE);
        if(freeRecPageId == 0){
            //allocate new
            freeRecPageId = pageman.allocate(Magic.FREELOGIDS_PAGE);
        }
        PageIo freeRecPage = file.get(freeRecPageId);
        //write all uncommited free records
        for(int rowPos = 0;rowPos<freeRecordsInTransSize;rowPos++){
            short count = freeRecPage.readShort(OFFSET_FREE_COUNT);
            if(count == FREE_RECORDS_PER_PAGE){
                //allocate new free recid page
                file.release(freeRecPage);
                freeRecPageId = pageman.allocate(Magic.FREELOGIDS_PAGE);
                freeRecPage = file.get(freeRecPageId);
                freeRecPage.writeShort(FREE_RECORDS_PER_PAGE, (short)0);
                count = 0;
            }
            final int offset =  (count ) *6 + FREE_HEADER_SIZE;
            //write free recid and increase counter
            freeRecPage.writeSixByteLong(offset,freeRecordsInTransRowid[rowPos]);
            count++;
            freeRecPage.writeShort(OFFSET_FREE_COUNT, count);

        }
        file.release(freeRecPage);

        clearFreeRecidsInTransaction();
    }

    private void clearFreeRecidsInTransaction() {
        if(freeRecordsInTransRowid.length>128)
            freeRecordsInTransRowid = new long[4];
        freeRecordsInTransSize = 0;
    }

    void rollback() throws IOException {
        clearFreeRecidsInTransaction();
    }


    /**
     * Returns a free Logical rowid, or
     * 0 if nothing was found.
     */
    long getFreeSlot() throws IOException {
        if (freeRecordsInTransSize != 0) {
            return freeRecordsInTransRowid[--freeRecordsInTransSize];
        }

        final long logicFreePageId = pageman.getLast(Magic.FREELOGIDS_PAGE);
        if(logicFreePageId == 0) {
            return 0;
        }
        PageIo logicFreePage = file.get(logicFreePageId);
        short recCount = logicFreePage.readShort(OFFSET_FREE_COUNT);
        if(recCount <= 0){
            throw new InternalError();
        }


        final int offset = (recCount -1) *6 + FREE_HEADER_SIZE;
        final long ret = logicFreePage.readSixByteLong(offset);

        recCount--;

        if(recCount>0){
            //decrease counter and zero out old record
            logicFreePage.writeSixByteLong(offset,0);
            logicFreePage.writeShort(OFFSET_FREE_COUNT, recCount);
            file.release(logicFreePage);
        }else{
            //release this page
            file.release(logicFreePage);
            pageman.free(Magic.FREELOGIDS_PAGE,logicFreePageId);
        }

        return ret;
    }

    /**
     * Puts the indicated rowid on the free list
     */
    void putFreeSlot(long rowid) throws IOException {
        //ensure capacity
        if(freeRecordsInTransSize == freeRecordsInTransRowid.length)
            freeRecordsInTransRowid = Arrays.copyOf(freeRecordsInTransRowid, freeRecordsInTransRowid.length * 4);
        //add record and increase size
        freeRecordsInTransRowid[freeRecordsInTransSize]=rowid;
        freeRecordsInTransSize++;
    }



}
