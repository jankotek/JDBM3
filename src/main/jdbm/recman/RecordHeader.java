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

/**
 *  The data that comes at the start of a record of data. It stores 
 *  both the current size and the avaliable size for the record - the latter
 *  can be bigger than the former, which allows the record to grow without
 *  needing to be moved and which allows the system to put small records
 *  in larger free spots.
 */
final class RecordHeader {
    // offsets
    private static final short O_CURRENTSIZE = 0; // int currentSize
    private static final short O_AVAILABLESIZE = Magic.SZ_INT; // int availableSize
    static final int SIZE = O_AVAILABLESIZE + Magic.SZ_INT;
    
    // my block and the position within the block
    final private BlockIo block;
    final private short pos;

    /**
     *  Constructs a record header from the indicated data starting at
     *  the indicated position.
     */
    RecordHeader(BlockIo block, short pos) {
        this.block = block;
        this.pos = pos;
        if (pos > (RecordFile.BLOCK_SIZE - SIZE))
            throw new Error("Offset too large for record header (" 
                            + block.getBlockId() + ":" 
                            + pos + ")");
    }

    /** Returns the current size */
    int getCurrentSize() {
        return block.readInt(pos + O_CURRENTSIZE);
    }
    
    /** Sets the current size */
    void setCurrentSize(int value) {
        block.writeInt(pos + O_CURRENTSIZE, value);
    }
    
    /** Returns the available size */
    int getAvailableSize() {
        return block.readInt(pos + O_AVAILABLESIZE);
    }
    
    /** Sets the available size */
    void setAvailableSize(int value) {
        block.writeInt(pos + O_AVAILABLESIZE, value);
    }

    // overrides java.lang.Object
    public String toString() {
        return "RH(" + block.getBlockId() + ":" + pos 
            + ", avl=" + getAvailableSize()
            + ", cur=" + getCurrentSize() 
            + ")";
    }
}
