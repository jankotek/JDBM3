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
 * $Id: FreePhysicalRowIdPage.java,v 1.4 2006/06/01 13:13:15 thompsonbry Exp $
 */

package jdbm.recman;

/**
 * Class describing a page that holds physical rowids that were freed.
 */
final class FreePhysicalRowIdPage extends PageHeader {
	// offsets
	private static final short O_COUNT = PageHeader.SIZE; // short count
	static final short O_FREE = O_COUNT + Magic.SZ_SHORT;
	static final short ELEMS_PER_PAGE = (RecordFile.BLOCK_SIZE - O_FREE) / FreePhysicalRowId.SIZE;

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

	// slots we returned.
	FreePhysicalRowId[] slots = new FreePhysicalRowId[ELEMS_PER_PAGE];

	/**
	 * Constructs a data page view from the indicated block.
	 */
	FreePhysicalRowIdPage(BlockIo block) {
		super(block);
	}

	/**
	 * Factory method to create or return a data page for the indicated block.
	 */
	static FreePhysicalRowIdPage getFreePhysicalRowIdPageView(BlockIo block) {
		BlockView view = block.getView();
		if (view != null && view instanceof FreePhysicalRowIdPage)
			return (FreePhysicalRowIdPage) view;
		else
			return new FreePhysicalRowIdPage(block);
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
		get(slot).setSize(0);
		setCount((short) (getCount() - 1));
	}

	/** Allocates a slot */
	FreePhysicalRowId alloc(int slot) {
		setCount((short) (getCount() + 1));
		return get(slot);
	}

	/** Returns true if a slot is allocated */
	boolean isAllocated(int slot) {
		return get(slot).getSize() != 0;
	}

	/** Returns true if a slot is free */
	boolean isFree(int slot) {
		return !isAllocated(slot);
	}

	/** Returns the value of the indicated slot */
	FreePhysicalRowId get(int slot) {
		if (slots[slot] == null) {
			slots[slot] = new FreePhysicalRowId(block, slotToOffset(slot));
		}
		return slots[slot];
	}

	/** Converts slot to offset */
	short slotToOffset(int slot) {
		return (short) (O_FREE + (slot * FreePhysicalRowId.SIZE));
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
	 * Returns first slot with available size >= indicated size, or -1 if no slots are available.
	 * 
	 * @param size
	 *            The requested allocation size.
	 **/
	int getFirstLargerThan(int size) {
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
			long theSize = get(i).getSize(); // capacity of this free record.
			long waste = theSize - size; // when non-negative, record has suf. capacity.
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
		return -1;
	}

}
