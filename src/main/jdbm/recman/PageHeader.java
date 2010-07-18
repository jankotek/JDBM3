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
 *  This class represents a page header. It is the common superclass for
 *  all different page views.
 */
class PageHeader implements BlockView {
    // offsets
    private static final short O_MAGIC = 0; // short magic
    private static final short O_NEXT = Magic.SZ_SHORT;  // long next
    private static final short O_PREV = O_NEXT + Magic.SZ_LONG; // long prev
    protected static final short SIZE = O_PREV + Magic.SZ_LONG;

    static final short PhysicalRowId_O_BLOCK = 0; // long block
    static final short PhysicalRowId_O_OFFSET = Magic.SZ_SIX_BYTE_LONG; // short offset
    static final int PhysicalRowId_SIZE = PhysicalRowId_O_OFFSET + Magic.SZ_SHORT;

    
    // my block
    protected BlockIo block;

    /**
     *  Constructs a PageHeader object from a block
     *
     *  @param block The block that contains the file header
     *  @throws IOException if the block is too short to keep the file
     *          header.
     */
    protected PageHeader(BlockIo block) {
        initialize(block);
        if (!magicOk())
            throw new Error("CRITICAL: page header magic for block "
                            + block.getBlockId() + " not OK "
                            + getMagic());
    }
    
    /**
     *  Constructs a new PageHeader of the indicated type. Used for newly
     *  created pages.
     */
    PageHeader(BlockIo block, short type) {
        initialize(block);
        setType(type);
    }
    
    /**
     *  Factory method to create or return a page header for the
     *  indicated block.
     */
    static PageHeader getView(BlockIo block) {
        BlockView view = block.getView();
        if (view != null && view instanceof PageHeader)
            return (PageHeader) view;
        else
            return new PageHeader(block);
    }
    
    private void initialize(BlockIo block) {
        this.block = block;
        block.setView(this);
    }
    
    /**
     *  Returns true if the magic corresponds with the fileHeader magic.
     */
    private boolean magicOk() {
        int magic = getMagic();
        return magic >= Magic.BLOCK
            && magic <= (Magic.BLOCK + Magic.FREEPHYSIDS_PAGE);
    }
    
    /**
     *  For paranoia mode
     */
    protected void paranoiaMagicOk() {
        if (!magicOk())
            throw new Error("CRITICAL: page header magic not OK "
                            + getMagic());
    }
    
    /** Returns the magic code */
    short getMagic() {
        return block.readShort(O_MAGIC);
    }

    /** Returns the next block. */
    long getNext() {
        paranoiaMagicOk();
        return block.readLong(O_NEXT);
    }
    
    /** Sets the next block. */
    void setNext(long next) {
        paranoiaMagicOk();
        block.writeLong(O_NEXT, next);
    }
    
    /** Returns the previous block. */
    long getPrev() {
        paranoiaMagicOk();
        return block.readLong(O_PREV);
    }
    
    /** Sets the previous block. */
    void setPrev(long prev) {
        paranoiaMagicOk();
        block.writeLong(O_PREV, prev);
    }
    
    /** Sets the type of the page header */
    void setType(short type) {
        block.writeShort(O_MAGIC, (short) (Magic.BLOCK + type));
    }
    
    /** Returns the block number */
    long getLocationBlock(short pos) {
        return block.readSixByteLong(pos + PhysicalRowId_O_BLOCK);
    }
    
    /** Sets the block number */
    void setLocationBlock(short pos,long value) {
        block.writeSixByteLong(pos + PhysicalRowId_O_BLOCK, value);
    }
    
    /** Returns the offset */
    short getLocationOffset(short pos) {
        return block.readShort(pos + PhysicalRowId_O_OFFSET);
    }
    
    /** Sets the offset */
    void setLocationOffset(short pos,short value) {
        block.writeShort(pos + PhysicalRowId_O_OFFSET, value);
    }


}
