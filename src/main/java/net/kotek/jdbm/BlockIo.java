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

import javax.crypto.Cipher;
import java.io.*;
import java.nio.ByteBuffer;

/**
 * This class wraps a page-sized byte array and provides methods
 * to read and write data to and from it. The readers and writers
 * are just the ones that the rest of the toolkit needs, nothing else.
 * Values written are compatible with java.io routines.
 *
 * @see java.io.DataInput
 * @see java.io.DataOutput
 */
final class BlockIo {

    private long blockId;

    private ByteBuffer data; // work area
    transient private BlockView view = null;
    private boolean dirty = false;
    private int transactionCount = 0;

    /**
     * Default constructor for serialization
     */
    public BlockIo() {
        // empty
    }

    /**
     * Constructs a new BlockIo instance working on the indicated
     * buffer.
     */
    BlockIo(long blockId, byte[] data) {
        this.blockId = blockId;
        this.data = ByteBuffer.wrap(data);
    }

    public BlockIo(long blockid, ByteBuffer data) {
        this.blockId = blockid;
        this.data = data;
    }

    /**
     * Returns the underlying array
     */
    ByteBuffer getData() {
        return data;
    }

    /**
     * Sets the block number. Should only be called by RecordFile.
     */
    void setBlockId(long id) {
        if (isInTransaction())
            throw new Error("BlockId assigned for transaction block");
        blockId = id;
    }

    /**
     * Returns the block number.
     */
    long getBlockId() {
        return blockId;
    }

    /**
     * Returns the current view of the block.
     */
    public BlockView getView() {
        return view;
    }

    /**
     * Sets the current view of the block.
     */
    public void setView(BlockView view) {
        this.view = view;
    }

    /**
     * Sets the dirty flag
     */
    void setDirty() {
        dirty = true;
        
        if(data.isReadOnly()){
            // make copy if needed, so we can write into buffer
            ByteBuffer old = data;
            data = ByteBuffer.allocate(Storage.BLOCK_SIZE);
            old.rewind();
            old.get(data.array(),0,Storage.BLOCK_SIZE);
            data.rewind();
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
    synchronized void incrementTransactionCount() {
        transactionCount++;
        // @fixme(alex)
        setClean();
    }

    /**
     * Decrements transaction count for this block, to signal that this
     * block has been written from the log to the data file.
     */
    synchronized void decrementTransactionCount() {
        transactionCount--;
        if (transactionCount < 0)
            throw new Error("transaction count on block "
                    + getBlockId() + " below zero!");

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
        return
                (((long) (data.get(pos + 0) & 0xff) << 40) |
                        ((long) (data.get(pos + 1) & 0xff) << 32) |
                        ((long) (data.get(pos + 2) & 0xff) << 24) |
                        ((long) (data.get(pos + 3) & 0xff) << 16) |
                        ((long) (data.get(pos + 4) & 0xff) << 8) |
                        ((long) (data.get(pos + 5) & 0xff) << 0));

    }

    /**
     * Writes a long to the indicated position
     */
    public void writeSixByteLong(int pos, long value) {
//    	if(value >> (6*8)!=0)
//    		throw new IllegalArgumentException("does not fit");
        setDirty();
        data.put(pos + 0,(byte) (0xff & (value >> 40)));
        data.put(pos + 1, (byte) (0xff & (value >> 32)));
        data.put(pos + 2, (byte) (0xff & (value >> 24)));
        data.put(pos + 3, (byte) (0xff & (value >> 16)));
        data.put(pos + 4, (byte) (0xff & (value >> 8)));
        data.put(pos + 5, (byte) (0xff & (value >> 0)));

    }


    // overrides java.lang.Object

    public String toString() {
        return "BlockIO("
                + blockId + ","
                + dirty + ","
                + view + ")";
    }

    // implement externalizable interface
    public void readExternal(DataInputStream in, Cipher cipherOut) throws IOException {
        blockId = LongPacker.unpackLong(in);
        byte[] data2 = new byte[Storage.BLOCK_SIZE];
        in.readFully(data2);
        if (cipherOut == null || Utils.allZeros(data2))
            data = ByteBuffer.wrap(data2);
        else try {
            data = ByteBuffer.wrap(cipherOut.doFinal(data2));
        } catch (Exception e) {
            throw new IOError(e);
        }
    }

    // implement externalizable interface
    public void writeExternal(DataOutput out, Cipher cipherIn) throws IOException {
        LongPacker.packLong(out, blockId);
        out.write(Utils.encrypt(cipherIn, data.array()));
    }


    public byte[] getByteArray() {
        if ( data.hasArray())
            return data.array();
        byte[] d= new byte[Storage.BLOCK_SIZE];
        data.rewind();
        data.get(d,0,Storage.BLOCK_SIZE);
        return d;
    }

    public void writeByteArray(byte[] buf, int srcOffset, int offset, int length) {
        setDirty();
        data.rewind();
        data.position(offset);
        data.put(buf,srcOffset,length);
    }
}
