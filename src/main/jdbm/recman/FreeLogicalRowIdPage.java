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
 *  Class describing a page that holds logical rowids that were freed. Note
 *  that the methods have *physical* rowids in their signatures - this is
 *  because logical and physical rowids are internally the same, only their
 *  external representation (i.e. in the client API) differs.
 */
final class FreeLogicalRowIdPage extends PageHeader {
    // offsets
    private static final short O_COUNT = PageHeader.SIZE; // short count
    static final short O_FREE = (short)(O_COUNT + Magic.SZ_SHORT);
    final short ELEMS_PER_PAGE;

    private int previousFoundFree = 0; // keeps track of the most recent found free slot so we can locate it again quickly 
    private int previousFoundAllocated = 0; // keeps track of the most recent found allocated slot so we can locate it again quickly
    
    // slots we returned.
    //final PhysicalRowId[] slots = new PhysicalRowId[ELEMS_PER_PAGE];

    /**
     *  Constructs a data page view from the indicated block.
     */
    FreeLogicalRowIdPage(BlockIo block, int blockSize) {
        super(block);
        ELEMS_PER_PAGE = (short) ((blockSize - O_FREE) / PhysicalRowId_SIZE);
    }

    /**
     *  Factory method to create or return a data page for the
     *  indicated block.
     */
    static FreeLogicalRowIdPage getFreeLogicalRowIdPageView(BlockIo block, int blockSize) {

        BlockView view = block.getView();
        if (view != null && view instanceof FreeLogicalRowIdPage)
            return (FreeLogicalRowIdPage) view;
        else
            return new FreeLogicalRowIdPage(block,blockSize);
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
    	setLocationBlock(slotToOffset(slot), 0);
        //get(slot).setBlock(0);
        setCount((short) (getCount() - 1));
        
        // update previousFoundFree if the freed slot is before what we've found in the past
        if (slot < previousFoundFree)
            previousFoundFree = slot;
    }

    /** Allocates a slot */
    short alloc(int slot) {
        setCount((short) (getCount() + 1));
        short pos = slotToOffset(slot);
        setLocationBlock(pos, -1);
        //get(slot).setBlock(-1);
        
        // update previousFoundAllocated if the newly allocated slot is before what we've found in the past
        if (slot < previousFoundAllocated)
            previousFoundAllocated = slot;
        
        return pos;
    }

    /** Returns true if a slot is allocated */
    boolean isAllocated(int slot) {
        //return get(slot).getBlock() > 0;
    	return getLocationBlock(slotToOffset(slot)) > 0;
    }

    /** Returns true if a slot is free */
    boolean isFree(int slot) {
        return !isAllocated(slot);
    }


//    /** Returns the value of the indicated slot */
//    PhysicalRowId get(int slot) {
//        if (slots[slot] == null)
//            slots[slot] = new PhysicalRowId(block, slotToOffset(slot));;
//        return slots[slot];
//    }
//    

    /** Converts slot to offset */
    short slotToOffset(int slot) {
        return (short) (O_FREE +
                        (slot * PhysicalRowId_SIZE));
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

    public long slotToLocation(int slot) {
		short pos = slotToOffset(slot);
		return Location.toLong(getLocationBlock(pos),getLocationOffset(pos));
	}
    

}
