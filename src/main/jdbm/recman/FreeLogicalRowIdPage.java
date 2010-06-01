/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot. 
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 * $Id: FreeLogicalRowIdPage.java,v 1.3 2005/11/08 20:58:28 thompsonbry Exp $
 */

package jdbm.recman;

/**
 *  Class describing a page that holds logical rowids that were freed. Note
 *  that the methods have *physical* rowids in their signatures - this is
 *  because logical and physical rowids are internally the same, only their
 *  external representation (i.e. in the client API) differs.
 */
final class FreeLogicalRowIdPage extends PageHeader {
    // offsets
    private static final short O_COUNT = PageHeader.SIZE; // short count
    static final short O_FREE = (short)(O_COUNT + Magic.SZ_SHORT);
    static final short ELEMS_PER_PAGE = (short)
        ((RecordFile.BLOCK_SIZE - O_FREE) / PhysicalRowId.SIZE);

    private int previousFoundFree = 0; // keeps track of the most recent found free slot so we can locate it again quickly 
    private int previousFoundAllocated = 0; // keeps track of the most recent found allocated slot so we can locate it again quickly
    
    // slots we returned.
    final PhysicalRowId[] slots = new PhysicalRowId[ELEMS_PER_PAGE];

    /**
     *  Constructs a data page view from the indicated block.
     */
    FreeLogicalRowIdPage(BlockIo block) {
        super(block);
    }

    /**
     *  Factory method to create or return a data page for the
     *  indicated block.
     */
    static FreeLogicalRowIdPage getFreeLogicalRowIdPageView(BlockIo block) {

        BlockView view = block.getView();
        if (view != null && view instanceof FreeLogicalRowIdPage)
            return (FreeLogicalRowIdPage) view;
        else
            return new FreeLogicalRowIdPage(block);
    }

    /** Returns the number of free rowids on this page. */
    short getCount() {
        return block.readShort(O_COUNT);
    }

    /** Sets the number of free rowids */
    private void setCount(short i) {
        block.writeShort(O_COUNT, i);
    }

    /** Frees a slot */
    void free(int slot) {
        get(slot).setBlock(0);
        setCount((short) (getCount() - 1));
        
        // update previousFoundFree if the freed slot is before what we've found in the past
        if (slot < previousFoundFree)
            previousFoundFree = slot;
    }

    /** Allocates a slot */
    PhysicalRowId alloc(int slot) {
        setCount((short) (getCount() + 1));
        get(slot).setBlock(-1);
        
        // update previousFoundAllocated if the newly allocated slot is before what we've found in the past
        if (slot < previousFoundAllocated)
            previousFoundAllocated = slot;
        
        return get(slot);
    }

    /** Returns true if a slot is allocated */
    boolean isAllocated(int slot) {
        return get(slot).getBlock() > 0;
    }

    /** Returns true if a slot is free */
    boolean isFree(int slot) {
        return !isAllocated(slot);
    }


    /** Returns the value of the indicated slot */
    PhysicalRowId get(int slot) {
        if (slots[slot] == null)
            slots[slot] = new PhysicalRowId(block, slotToOffset(slot));;
        return slots[slot];
    }

    /** Converts slot to offset */
    private short slotToOffset(int slot) {
        return (short) (O_FREE +
                        (slot * PhysicalRowId.SIZE));
    }

    /**
     *  Returns first free slot, -1 if no slots are available
     */
    int getFirstFree() {
        for (; previousFoundFree < ELEMS_PER_PAGE; previousFoundFree++) {
            if (isFree(previousFoundFree))
                return previousFoundFree;
        }
        return -1;
    }

    /**
     *  Returns first allocated slot, -1 if no slots are available.
     */
    int getFirstAllocated() {
        for (; previousFoundAllocated < ELEMS_PER_PAGE; previousFoundAllocated++) {
            if (isAllocated(previousFoundAllocated))
                return previousFoundAllocated;
        }
        return -1;
    }
}
