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

import java.io.IOException;

/**
 *  This class represents a file header. It is a 1:1 representation of
 *  the data that appears in block 0 of a file.
 */
class FileHeader implements BlockView {
    // offsets
    private static final short O_MAGIC = 0; // short magic
    private static final short O_LISTS = Magic.SZ_SHORT; // long[2*NLISTS]
    private static final int O_ROOTS = 
        O_LISTS + (Magic.NLISTS * 2 * Magic.SZ_LONG);

    // my block
    private BlockIo block;

    /** The number of "root" rowids available in the file. */
    static final int NROOTS = (1024 - O_ROOTS) / Magic.SZ_LONG;
    	//FIXME should this be dynamic
        //(RecordFile.BLOCK_SIZE - O_ROOTS) / Magic.SZ_LONG;

    /**
     *  Constructs a FileHeader object from a block.
     *
     *  @param block The block that contains the file header
     *  @param isNew If true, the file header is for a new file.
     *  @throws IOException if the block is too short to keep the file
     *          header.
     */
    FileHeader(BlockIo block, boolean isNew) {
        this.block = block;
        if (isNew)
            block.writeShort(O_MAGIC, Magic.FILE_HEADER);
        else if (!magicOk())
            throw new Error("CRITICAL: file header magic not OK " 
                            + block.readShort(O_MAGIC));
    }

    /** Returns true if the magic corresponds with the fileHeader magic.  */
    private boolean magicOk() {
        return block.readShort(O_MAGIC) == Magic.FILE_HEADER;
    }


    /** Returns the offset of the "first" block of the indicated list */
    private short offsetOfFirst(int list) {
        return (short) (O_LISTS + (2 * Magic.SZ_LONG * list));
    }

    /** Returns the offset of the "last" block of the indicated list */
    private short offsetOfLast(int list) {
        return (short) (offsetOfFirst(list) + Magic.SZ_LONG);
    }

    /** Returns the offset of the indicated root */
    private short offsetOfRoot(int root) {
        return (short) (O_ROOTS + (root * Magic.SZ_LONG));
    }

    /**
     *  Returns the first block of the indicated list
     */
    long getFirstOf(int list) {
        return block.readLong(offsetOfFirst(list));
    }
    
    /**
     *  Sets the first block of the indicated list
     */
    void setFirstOf(int list, long value) {
        block.writeLong(offsetOfFirst(list), value);
    }
    
    /**
     *  Returns the last block of the indicated list
     */
    long getLastOf(int list) {
        return block.readLong(offsetOfLast(list));
    }
    
    /**
     *  Sets the last block of the indicated list
     */
    void setLastOf(int list, long value) {
        block.writeLong(offsetOfLast(list), value);
    }

    /**
     *  Returns the indicated root rowid. A root rowid is a special rowid
     *  that needs to be kept between sessions. It could conceivably be
     *  stored in a special file, but as a large amount of space in the
     *  block header is wasted anyway, it's more useful to store it where
     *  it belongs.
     *
     *  @see #NROOTS
     */
    long getRoot(int root) {
        return block.readLong(offsetOfRoot(root));
    }

    /**
     *  Sets the indicated root rowid.
     *
     *  @see #getRoot
     *  @see #NROOTS
     */
    void setRoot(int root, long rowid) {
        block.writeLong(offsetOfRoot(root), rowid);
    }
}
