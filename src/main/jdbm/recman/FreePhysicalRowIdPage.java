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
 * Class describing a page that holds physical rowids that were freed.
 */
final class FreePhysicalRowIdPage extends PageHeader {
	
	static final short FreePhysicalRowId_O_SIZE = PhysicalRowId_SIZE; // int size
	static final short FreePhysicalRowId_SIZE = FreePhysicalRowId_O_SIZE + Magic.SZ_INT;

	// offsets
	private static final short O_COUNT = PageHeader.SIZE; // short count
	static final short O_FREE = O_COUNT + Magic.SZ_SHORT;
	final short ELEMS_PER_PAGE;
	


	/**
	 * Used to place a limit on the wasted capacity resulting in a modified first fit policy for re-allocated of free
	 * records. This value is the maximum first fit waste that is accepted when scanning the available slots on a given
	 * page of the free physical row page list.
	 */
	static public final transient int wasteMargin = 128;

	/**
	 * Used to place a limit on the wasted capacity resulting in a modified first fit policy for re-allocated of free
	 * records. This value is the upper bound of waste that is accepted before scanning another page on the free
	 * physical row page list. If no page can be found whose waste for the re-allocation request would be less than this
	 * value then a new page will be allocated and the requested physical row will be allocated from that new page.
	 */
	static public final transient int wasteMargin2 = PageHeader.SIZE / 4;

//	// slots we returned.
//	FreePhysicalRowId[] slots = new FreePhysicalRowId[ELEMS_PER_PAGE];

	final int[] sizeCache;
	
	/**
	 * Constructs a data page view from the indicated block.
	 */
	FreePhysicalRowIdPage(BlockIo block, int blockSize) {
		super(block);
		ELEMS_PER_PAGE = (short) ((blockSize - O_FREE) / FreePhysicalRowId_SIZE);
		 sizeCache = new int[ELEMS_PER_PAGE];
		for(int i = 0;i<ELEMS_PER_PAGE;i++)
			sizeCache[i] = -1;
		
	}

	/**
	 * Factory method to create or return a data page for the indicated block.
	 */
	static FreePhysicalRowIdPage getFreePhysicalRowIdPageView(BlockIo block,int pageSize) {
		BlockView view = block.getView();
		if (view != null && view instanceof FreePhysicalRowIdPage)
			return (FreePhysicalRowIdPage) view;
		else
			return new FreePhysicalRowIdPage(block, pageSize);
	}

	/** Returns the number of free rowids */
	short getCount() {
		return block.readShort(O_COUNT);
	}

	/** Sets the number of free rowids */
	private void setCount(short i) {
		block.writeShort(O_COUNT, i);
	}

	/** Frees a slot */
	void free(int slot) {
		short pos = slotToOffset(slot);
		FreePhysicalRowId_setSize(pos, 0);
		//get(slot).setSize(0);
		setCount((short) (getCount() - 1));
	}

	/** Allocates a slot */
	short alloc(int slot) {
		setCount((short) (getCount() + 1));
		return slotToOffset(slot);
	}

	/** Returns true if a slot is allocated */
	boolean isAllocated(int slot) {
		short pos = slotToOffset(slot);
		return FreePhysicalRowId_getSize(pos) != 0;
	}

	/** Returns true if a slot is free */
	boolean isFree(int slot) {
		return !isAllocated(slot);
	}

//	/** Returns the value of the indicated slot */
//	FreePhysicalRowId get(int slot) {
//		if (slots[slot] == null) {
//			slots[slot] = new FreePhysicalRowId(block, slotToOffset(slot));
//		}
//		return slots[slot];
//	}

	/** Converts slot to offset */
	short slotToOffset(int slot) {
		return (short) (O_FREE + (slot * FreePhysicalRowId_SIZE));
	}
	
	int offsetToSlot(short pos) {
		int pos2 = pos;
		return (pos2 - O_FREE)/FreePhysicalRowId_SIZE;			
	}


	/**
	 * Returns first free slot, -1 if no slots are available
	 */
	int getFirstFree() {
		for (int i = 0; i < ELEMS_PER_PAGE; i++) {
			if (isFree(i))
				return i;
		}
		return -1;
	}

	/**
	 * Returns first slot with available size >= indicated size, or minus maximal size available on this page
	 * 
	 * @param size
	 *            The requested allocation size.
         *
	 **/
	int getFirstLargerThan(int size) {

                int maxSize = 0;
		/*
		 * Tracks slot of the smallest available physical row on the page.
		 */
		int bestSlot = -1;
		/*
		 * Tracks size of the smallest available physical row on the page.
		 */
		int bestSlotSize = 0;
		/*
		 * Scan each slot in the page.
		 */
		for (int i = 0; i < ELEMS_PER_PAGE; i++) {
			/*
			 * When large allocations are used, the space wasted by the first fit policy can become very large (25% of
			 * the store). The first fit policy has been modified to only accept a record with a maximum amount of
			 * wasted capacity given the requested allocation size.
			 */
			// Note: isAllocated(i) is equiv to get(i).getSize() != 0
			//long theSize = get(i).getSize(); // capacity of this free record.
			short pos = slotToOffset(i);
			int theSize = FreePhysicalRowId_getSize(pos); // capacity of this free record.
                        if(theSize>maxSize) maxSize = theSize;
			int waste = theSize - size; // when non-negative, record has suf. capacity.
			if (waste >= 0) {
				if (waste < wasteMargin) {
					return i; // record has suf. capacity and not too much waste.
				} else if (bestSlotSize >= size) {
					/*
					 * This slot is a better fit that any that we have seen so far on this page so we update the slot#
					 * and available size for that slot.
					 */
					bestSlot = i;
					bestSlotSize = size;
				}
			}
		}
		if (bestSlot != -1) {
			/*
			 * An available slot was identified that is large enough, but it exceeds the first wasted capacity limit. At
			 * this point we check to see whether it is under our second wasted capacity limit. If it is, then we return
			 * that slot.
			 */
			long waste = bestSlotSize - size; // when non-negative, record has suf. capacity.
			if (waste >= 0 && waste < wasteMargin2) {
				// record has suf. capacity and not too much waste.
				return bestSlot;
			}
			/*
			 * Will scan next page on the free physical row page list.
			 */
		}

		return -maxSize;
	}

	public long slotToLocation(int slot) {
		short pos = slotToOffset(slot);
		return Location.toLong(getLocationBlock(pos),getLocationOffset(pos));
	}
	
	/** Returns the size */
	int FreePhysicalRowId_getSize(short pos) {
		int slot = offsetToSlot(pos);
		if(sizeCache[slot] == -1)
			sizeCache[slot] =  block.readInt(pos + FreePhysicalRowId_O_SIZE);
		return sizeCache[slot];
	}

	/** Sets the size */
	void FreePhysicalRowId_setSize(short pos,int value) {
		int slot = offsetToSlot(pos);
		sizeCache[slot] = value;
		block.writeInt(pos + FreePhysicalRowId_O_SIZE, value);
	}


}
