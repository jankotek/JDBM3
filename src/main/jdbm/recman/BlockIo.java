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


package jdbm.recman;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import jdbm.helper.LongPacker;

/**
 *  This class wraps a page-sized byte array and provides methods
 *  to read and write data to and from it. The readers and writers
 *  are just the ones that the rest of the toolkit needs, nothing else.
 *  Values written are compatible with java.io routines.
 *
 *  @see java.io.DataInput
 *  @see java.io.DataOutput
 */
public final class BlockIo {

    private long blockId;

    private byte[] data; // work area
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
     *  Constructs a new BlockIo instance working on the indicated
     *  buffer.
     */
    BlockIo(long blockId, byte[] data) {
        this.blockId = blockId;
        this.data = data;
    }

    /**
     *  Returns the underlying array
     */
    byte[] getData() {
        return data;
    }

    /**
     *  Sets the block number. Should only be called by RecordFile.
     */
    void setBlockId(long id) {
        if (isInTransaction())
            throw new Error("BlockId assigned for transaction block");
        blockId = id;
    }

    /**
     *  Returns the block number.
     */
    long getBlockId() {
        return blockId;
    }

    /**
     *  Returns the current view of the block.
     */
    public BlockView getView() {
        return view;
    }

    /**
     *  Sets the current view of the block.
     */
    public void setView(BlockView view) {
        this.view = view;
    }

    /**
     *  Sets the dirty flag
     */
    void setDirty() {
        dirty = true;
    }

    /**
     *  Clears the dirty flag
     */
    void setClean() {
        dirty = false;
    }

    /**
     *  Returns true if the dirty flag is set.
     */
    boolean isDirty() {
        return dirty;
    }

    /**
     *  Returns true if the block is still dirty with respect to the
     *  transaction log.
     */
    boolean isInTransaction() {
        return transactionCount != 0;
    }

    /**
     *  Increments transaction count for this block, to signal that this
     *  block is in the log but not yet in the data file. The method also
     *  takes a snapshot so that the data may be modified in new transactions.
     */
    synchronized void incrementTransactionCount() {
        transactionCount++;
        // @fixme(alex)
        setClean();
    }

    /**
     *  Decrements transaction count for this block, to signal that this
     *  block has been written from the log to the data file.
     */
    synchronized void decrementTransactionCount() {
        transactionCount--;
        if (transactionCount < 0)
            throw new Error("transaction count on block "
                            + getBlockId() + " below zero!");

    }

    /**
     *  Reads a byte from the indicated position
     */
    public byte readByte(int pos) {
        return data[pos];
    }

    /**
     *  Writes a byte to the indicated position
     */
    public void writeByte(int pos, byte value) {
        data[pos] = value;
        setDirty();
    }

    /**
     *  Reads a short from the indicated position
     */
    public short readShort(int pos) {
        return (short)
            (((short) (data[pos+0] & 0xff) << 8) |
             ((short) (data[pos+1] & 0xff) << 0));
    }

    /**
     *  Writes a short to the indicated position
     */
    public void writeShort(int pos, short value) {
        data[pos+0] = (byte)(0xff & (value >> 8));
        data[pos+1] = (byte)(0xff & (value >> 0));
        setDirty();
    }

    /**
     *  Reads an int from the indicated position
     */
    public int readInt(int pos) {
        return
            (((int)(data[pos+0] & 0xff) << 24) |
             ((int)(data[pos+1] & 0xff) << 16) |
             ((int)(data[pos+2] & 0xff) <<  8) |
             ((int)(data[pos+3] & 0xff) <<  0));
    }

    /**
     *  Writes an int to the indicated position
     */
    public void writeInt(int pos, int value) {
        data[pos+0] = (byte)(0xff & (value >> 24));
        data[pos+1] = (byte)(0xff & (value >> 16));
        data[pos+2] = (byte)(0xff & (value >>  8));
        data[pos+3] = (byte)(0xff & (value >>  0));
        setDirty();
    }
    
    static final int ThreeByteInt_MAX = 256 * 256 * 256 -1;
    
    /**
     *  Reads an int from the indicated position
     */
    public int readThreeByteInt(int pos) {
        return
            (
             ((int)(data[pos+0] & 0xff) << 16) |
             ((int)(data[pos+1] & 0xff) <<  8) |
             ((int)(data[pos+2] & 0xff) <<  0));
    }

    /**
     *  Writes an int to the indicated position
     */
    public void writeThreeByteInt(int pos, int value) {
    	if(value<0 || value>ThreeByteInt_MAX)
    		throw new IllegalArgumentException("out of range: "+value);
        data[pos+0] = (byte)(0xff & (value >> 16));
        data[pos+1] = (byte)(0xff & (value >>  8));
        data[pos+2] = (byte)(0xff & (value >>  0));
        setDirty();
    }

    /**
     *  Reads a long from the indicated position
     */
    public long readLong( int pos )
    {
        // Contributed by Erwin Bolwidt <ejb@klomp.org>
        // Gives about 15% performance improvement
        return
            ( (long)( ((data[pos+0] & 0xff) << 24) |
                      ((data[pos+1] & 0xff) << 16) |
                      ((data[pos+2] & 0xff) <<  8) |
                      ((data[pos+3] & 0xff)      ) ) << 32 ) |
            ( (long)( ((data[pos+4] & 0xff) << 24) |
                      ((data[pos+5] & 0xff) << 16) |
                      ((data[pos+6] & 0xff) <<  8) |
                      ((data[pos+7] & 0xff)      ) ) & 0xffffffff );
        /* Original version by Alex Boisvert.  Might be faster on 64-bit JVMs.
        return
            (((long)(data[pos+0] & 0xff) << 56) |
             ((long)(data[pos+1] & 0xff) << 48) |
             ((long)(data[pos+2] & 0xff) << 40) |
             ((long)(data[pos+3] & 0xff) << 32) |
             ((long)(data[pos+4] & 0xff) << 24) |
             ((long)(data[pos+5] & 0xff) << 16) |
             ((long)(data[pos+6] & 0xff) <<  8) |
             ((long)(data[pos+7] & 0xff) <<  0));
        */
    }

    /**
     *  Writes a long to the indicated position
     */
    public void writeLong(int pos, long value) {
        data[pos+0] = (byte)(0xff & (value >> 56));
        data[pos+1] = (byte)(0xff & (value >> 48));
        data[pos+2] = (byte)(0xff & (value >> 40));
        data[pos+3] = (byte)(0xff & (value >> 32));
        data[pos+4] = (byte)(0xff & (value >> 24));
        data[pos+5] = (byte)(0xff & (value >> 16));
        data[pos+6] = (byte)(0xff & (value >>  8));
        data[pos+7] = (byte)(0xff & (value >>  0));
        setDirty();
    }


    /**
     *  Reads a long from the indicated position
     */
    public long readSixByteLong( int pos )
    {
        return
            (((long)(data[pos+0] & 0xff) << 40) |
             ((long)(data[pos+1] & 0xff) << 32) |
             ((long)(data[pos+2] & 0xff) << 24) |
             ((long)(data[pos+3] & 0xff) << 16) |
             ((long)(data[pos+4] & 0xff) << 8) |
             ((long)(data[pos+5] & 0xff) << 0)); 
        
    }

    /**
     *  Writes a long to the indicated position
     */
    public void writeSixByteLong(int pos, long value) {
//    	if(value >> (6*8)!=0)
//    		throw new IllegalArgumentException("does not fit");
    	
        data[pos+0] = (byte)(0xff & (value >> 40));
        data[pos+1] = (byte)(0xff & (value >> 32));
        data[pos+2] = (byte)(0xff & (value >> 24));
        data[pos+3] = (byte)(0xff & (value >> 16));
        data[pos+4] = (byte)(0xff & (value >> 8 ));
        data[pos+5] = (byte)(0xff & (value >> 0 ));
        setDirty();
    }

    
    // overrides java.lang.Object

    public String toString() {
        return "BlockIO("
            + blockId + ","
            + dirty + ","
            + view + ")";
    }

    // implement externalizable interface
    public void readExternal(DataInputStream in)
    throws IOException, ClassNotFoundException {
        blockId = LongPacker.unpackLong(in);
        int length = LongPacker.unpackInt(in);
        data = new byte[length];
        in.readFully(data);
    }

    // implement externalizable interface
    public void writeExternal(DataOutputStream out) throws IOException {
    	LongPacker.packLong(out, blockId);
    	LongPacker.packInt(out, data.length);
        out.write(data);
    }

    static final int UNSIGNED_SHORT_MAX = 256 * 256 -1;  

   
	public void writeUnsignedShort(int pos, int value) {
		if(value>UNSIGNED_SHORT_MAX || value<0)
			throw new IllegalArgumentException("Out of range: "+value);
        data[pos+0] = (byte)(0xff & (value >>  8));
        data[pos+1] = (byte)(0xff & (value >>  0));
        setDirty();		
	}
	
	public int readUnsignedshort(int pos){
      return 
        (((int)(data[pos+0] & 0xff) <<  8) |
        ((int)(data[pos+1] & 0xff) <<  0));
	}

}
