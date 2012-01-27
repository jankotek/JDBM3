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

/**
 * This class represents a location within a file. Both physical and
 * logical rowids are based on locations internally - this version is
 * used when there is no file block to back the location's data.
 */
final class Location {   


    static long getBlock(final long loc) {
        return loc >>> Storage.BLOCK_SIZE_SHIFT;
    }

    static short getOffset(final long loc) {
        return (short) (loc & Storage.OFFSET_MASK);
    }

    static long toLong(final long block, final short offset) {
        return (block << Storage.BLOCK_SIZE_SHIFT) + (long) offset;
    }


    private static int COMPRESS_RECID_BLOCK_SHIFT;
    private static long COMPRESS_RECID_OFFSET_MASK;

    static{
        int shift = 1;
        while((1<<shift) <LogicalRowIdManager.ELEMS_PER_PAGE )
            shift++;
        COMPRESS_RECID_BLOCK_SHIFT = shift;
        COMPRESS_RECID_OFFSET_MASK =  0xFFFFFFFFFFFFFFFFL >>> (64-shift);
    }


    /**
     * Compress recid from physical form (block - offset) to (block - slot).
     * This way resulting number is smaller and can be easier packed with LongPacker
     */
    static long compressRecid(final long recid) {
        long block = Location.getBlock(recid);
        short offset = Location.getOffset(recid);

        offset = (short) (offset - Magic.PAGE_HEADER_SIZE);
        if (offset % Magic.PhysicalRowId_SIZE != 0)
            throw new InternalError("recid not dividable "+Magic.PhysicalRowId_SIZE);
        long slot = offset / Magic.PhysicalRowId_SIZE;

        return (block << COMPRESS_RECID_BLOCK_SHIFT) + slot;

    }

    static long decompressRecid(final long recid) {

        final long block = recid >>> COMPRESS_RECID_BLOCK_SHIFT;
        final short offset = (short) ((recid & COMPRESS_RECID_OFFSET_MASK) * Magic.PhysicalRowId_SIZE + Magic.PAGE_HEADER_SIZE);
        return Location.toLong(block, offset);
    }


}
