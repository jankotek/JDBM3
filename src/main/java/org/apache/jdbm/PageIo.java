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

import javax.crypto.Cipher;
import java.io.*;
import java.nio.ByteBuffer;
import static org.apache.jdbm.Magic.*;

/**
 * Wraps a page sizes ByteBuffer for reading and writing.
 * <p>
 * ByteBuffer may be subview of a larger buffer (ie large buffer mapped over a file).
 * In this case ByteBuffer will have set limit, mark and other variables to limit its size.
 * <p>
 * For reading buffered may be shared. For example StoreMemory just returns its pages without copying.
 * In this case buffer is marked as 'readonly' and needs to be copied before write (Copy On Write - COW).
 * COW is not necessary if transactions are disabled and changes can not be rolled back.
 * <p>

 */
final class PageIo {

    private long pageId;

    private ByteBuffer data; // work area

    /** buffers contains changes which were not written to disk yet. */
    private boolean dirty = false;

    private int transactionCount = 0;

    /**
     * Default constructor for serialization
     */
    public PageIo() {
        // empty
    }

    /**
     * Constructs a new PageIo instance working on the indicated
     * buffer.
     */
    PageIo(long pageId, byte[] data) {
        this.pageId = pageId;
        this.data = ByteBuffer.wrap(data);
    }

    public PageIo(long pageId, ByteBuffer data) {
        this.pageId = pageId;
        this.data = data;
    }

    /** Frequent reads on direct buffer may be slower then on heap buffer.
     * This method converts native direct to heap buffer
     */
    void ensureHeapBuffer(){
        if(data.isDirect()){
            final byte[] bb = new byte[Storage.PAGE_SIZE];
            data.get(bb,0,Storage.PAGE_SIZE);
            data = ByteBuffer.wrap(bb);
            if(data.isReadOnly()) throw new InternalError();
        }

    }

    /**
     * Returns the underlying array
     */
    ByteBuffer getData() {
        return data;
    }

    /**
     * Returns the page number.
     */
    long getPageId() {
        return pageId;
    }

    /**
     * Sets the dirty flag
     */
    void setDirty() {
        dirty = true;
        
        if(data.isReadOnly()){
            // make copy if needed, so we can write into buffer
            byte[] buf = new byte[Storage.PAGE_SIZE];
            data.get(buf,0,Storage.PAGE_SIZE);
            data = ByteBuffer.wrap(buf);
        }
    }

    /**
     * Clears the dirty flag
     */
    void setClean() {
        dirty = false;
    }

    /**
     * Returns true if the dirty flag is set.
     */
    boolean isDirty() {
        return dirty;
    }

    /**
     * Returns true if the block is still dirty with respect to the
     * transaction log.
     */
    boolean isInTransaction() {
        return transactionCount != 0;
    }

    /**
     * Increments transaction count for this block, to signal that this
     * block is in the log but not yet in the data file. The method also
     * takes a snapshot so that the data may be modified in new transactions.
     */
    void incrementTransactionCount() {
        transactionCount++;
    }

    /**
     * Decrements transaction count for this block, to signal that this
     * block has been written from the log to the data file.
     */
    void decrementTransactionCount() {
        transactionCount--;
        if (transactionCount < 0)
            throw new Error("transaction count on page "
                    + getPageId() + " below zero!");

    }

    /**
     * Reads a byte from the indicated position
     */
    public byte readByte(int pos) {
        return data.get(pos);
    }

    /**
     * Writes a byte to the indicated position
     */
    public void writeByte(int pos, byte value) {
        setDirty();
        data.put(pos,value);
    }

    /**
     * Reads a short from the indicated position
     */
    public short readShort(int pos) {
        return data.getShort(pos);
    }

    /**
     * Writes a short to the indicated position
     */
    public void writeShort(int pos, short value) {
        setDirty();
        data.putShort(pos,value);
    }

    /**
     * Reads an int from the indicated position
     */
    public int readInt(int pos) {
        return data.getInt(pos);
    }

    /**
     * Writes an int to the indicated position
     */
    public void writeInt(int pos, int value) {
        setDirty();
        data.putInt(pos,value);
    }

    /**
     * Reads a long from the indicated position
     */
    public long readLong(int pos) {
        return data.getLong(pos);
    }

    /**
     * Writes a long to the indicated position
     */
    public void writeLong(int pos, long value) {
        setDirty();
        data.putLong(pos,value);
    }


    /**
     * Reads a long from the indicated position
     */
    public long readSixByteLong(int pos) {
        long ret =                 
                ((long) (data.get(pos + 0) & 0x7f) << 40) |
                ((long) (data.get(pos + 1) & 0xff) << 32) |
                ((long) (data.get(pos + 2) & 0xff) << 24) |
                ((long) (data.get(pos + 3) & 0xff) << 16) |
                ((long) (data.get(pos + 4) & 0xff) << 8) |
                ((long) (data.get(pos + 5) & 0xff) << 0);
        if((data.get(pos + 0) & 0x80) != 0)
            return -ret;
        else
            return ret;

    }

    /**
     * Writes a long to the indicated position
     */
    public void writeSixByteLong(int pos, long value) {
//        if(value<0) throw new IllegalArgumentException();
//    	if(value >> (6*8)!=0)
//    		throw new IllegalArgumentException("does not fit");
        int negativeBit = 0;
        if(value<0){
            value = -value;
            negativeBit = 0x80;
        }

        setDirty();
        data.put(pos + 0,(byte) ((0x7f & (value >> 40)) | negativeBit));
        data.put(pos + 1, (byte) (0xff & (value >> 32)));
        data.put(pos + 2, (byte) (0xff & (value >> 24)));
        data.put(pos + 3, (byte) (0xff & (value >> 16)));
        data.put(pos + 4, (byte) (0xff & (value >> 8)));
        data.put(pos + 5, (byte) (0xff & (value >> 0)));

    }


    // overrides java.lang.Object

    public String toString() {
        return "PageIo("
                + pageId + ","
                + dirty +")";
    }

    public void readExternal(DataInputStream in, Cipher cipherOut) throws IOException {
        pageId = in.readLong();
        byte[] data2 = new byte[Storage.PAGE_SIZE];
        in.readFully(data2);
        if (cipherOut == null || Utils.allZeros(data2))
            data = ByteBuffer.wrap(data2);
        else try {
            data = ByteBuffer.wrap(cipherOut.doFinal(data2));
        } catch (Exception e) {
            throw new IOError(e);
        }
    }


    public void writeExternal(DataOutput out, Cipher cipherIn) throws IOException {
        out.writeLong(pageId);
        out.write(Utils.encrypt(cipherIn, data.array()));
    }


    public byte[] getByteArray() {
        if ( data.hasArray())
            return data.array();
        byte[] d= new byte[Storage.PAGE_SIZE];
        data.rewind();
        data.get(d,0,Storage.PAGE_SIZE);
        return d;
    }

    public void writeByteArray(byte[] buf, int srcOffset, int offset, int length) {
        setDirty();
        data.rewind();
        data.position(offset);
        data.put(buf,srcOffset,length);
    }
    
    public void fileHeaderCheckHead(boolean isNew){
        if (isNew)
            writeShort(FILE_HEADER_O_MAGIC, Magic.FILE_HEADER);
        else{
            short magic = readShort(FILE_HEADER_O_MAGIC);
            if(magic!=FILE_HEADER)
                throw new Error("CRITICAL: file header magic not OK " + magic);
       }
    }

    /**
     * Returns the first page of the indicated list
     */
    long fileHeaderGetFirstOf(int list) {
        return readLong(fileHeaderOffsetOfFirst(list));
    }

    /**
     * Sets the first page of the indicated list
     */
    void fileHeaderSetFirstOf(int list, long value) {
        writeLong(fileHeaderOffsetOfFirst(list), value);
    }

    /**
     * Returns the last page of the indicated list
     */
    long fileHeaderGetLastOf(int list) {
        return readLong(fileHeaderOffsetOfLast(list));
    }

    /**
     * Sets the last page of the indicated list
     */
    void fileHeaderSetLastOf(int list, long value) {
        writeLong(fileHeaderOffsetOfLast(list), value);
    }


    /**
     * Returns the offset of the "first" page of the indicated list
     */
    private short fileHeaderOffsetOfFirst(int list) {
        return (short) (FILE_HEADER_O_LISTS + (2 * Magic.SZ_LONG * list));
    }

    /**
     * Returns the offset of the "last" page of the indicated list
     */
    private short fileHeaderOffsetOfLast(int list) {
        return (short) (fileHeaderOffsetOfFirst(list) + Magic.SZ_LONG);
    }


    
    /**
     * Returns the indicated root rowid. A root rowid is a special rowid
     * that needs to be kept between sessions. It could conceivably be
     * stored in a special file, but as a large amount of space in the
     * page header is wasted anyway, it's more useful to store it where
     * it belongs.
     *

     */
    long fileHeaderGetRoot(final int root) {
        final short offset = (short) (FILE_HEADER_O_ROOTS + (root * Magic.SZ_LONG));
        return readLong(offset);
    }

    /**
     * Sets the indicated root rowid.
     *
     */
    void fileHeaderSetRoot(final int root, final long rowid) {
        final short offset = (short) (FILE_HEADER_O_ROOTS + (root * Magic.SZ_LONG));
        writeLong(offset, rowid);
    }


    /**
     * Returns true if the magic corresponds with the fileHeader magic.
     */
    boolean pageHeaderMagicOk() {
        int magic = pageHeaderGetMagic();
        return magic >= Magic.PAGE_MAGIC && magic <= (Magic.PAGE_MAGIC + Magic.FREEPHYSIDS_ROOT_PAGE);
    }

    /**
     * For paranoia mode
     */
    protected void pageHeaderParanoiaMagicOk() {
        if (!pageHeaderMagicOk())
            throw new Error("CRITICAL: page header magic not OK " + pageHeaderGetMagic());
    }

    short pageHeaderGetMagic() {
        return readShort(PAGE_HEADER_O_MAGIC);
    }

    long pageHeaderGetNext() {
        pageHeaderParanoiaMagicOk();
        return readSixByteLong(PAGE_HEADER_O_NEXT);
    }

    void pageHeaderSetNext(long next) {
        pageHeaderParanoiaMagicOk();
        writeSixByteLong(PAGE_HEADER_O_NEXT, next);
    }

    long pageHeaderGetPrev() {
        pageHeaderParanoiaMagicOk();
        return readSixByteLong(PAGE_HEADER_O_PREV);
    }


    void pageHeaderSetPrev(long prev) {
        pageHeaderParanoiaMagicOk();
        writeSixByteLong(PAGE_HEADER_O_PREV, prev);
    }
    void pageHeaderSetType(short type) {
        writeShort(PAGE_HEADER_O_MAGIC, (short) (Magic.PAGE_MAGIC + type));
    }

    long pageHeaderGetLocation(final short pos){
        return readSixByteLong(pos + PhysicalRowId_O_LOCATION);
    }


    void pageHeaderSetLocation(short pos, long value) {
       writeSixByteLong(pos + PhysicalRowId_O_LOCATION, value);
    }



    short dataPageGetFirst() {
        return readShort(DATA_PAGE_O_FIRST);
    }

    void dataPageSetFirst(short value) {
        pageHeaderParanoiaMagicOk();
        if (value > 0 && value < DATA_PAGE_O_DATA)
            throw new Error("DataPage.setFirst: offset " + value + " too small");
        writeShort(DATA_PAGE_O_FIRST, value);
    }




}
