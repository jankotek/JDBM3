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
import java.util.ArrayList;
import java.util.Iterator;

/**
 * This class manages free physical rowid pages and provides methods to free and allocate physical rowids on a high
 * level.
 */
final class FreePhysicalRowIdPageManager {
	// our record file
	protected RecordFile _file;

	// our page manager
	protected PageManager _pageman;

	private int blockSize;

        /** if true, new records are always placed to end of file, new space is not reclaimed */
        private boolean appendToEnd = false;
	
	final ArrayList<Long> freeBlocksInTransactionRowid = new ArrayList<Long>();
	final ArrayList<Integer> freeBlocksInTransactionSize = new ArrayList<Integer>();

	/**
	 * Creates a new instance using the indicated record file and page manager.
	 */
	FreePhysicalRowIdPageManager(RecordFile file, PageManager pageman, boolean append) throws IOException {
		_file = file;
		_pageman = pageman;
		this.blockSize = file.BLOCK_SIZE;
                this.appendToEnd = append;

	}

        private int lastMaxSize = -1;

	/**
	 * Returns a free physical rowid of the indicated size, or null if nothing was found. This scans the free physical
	 * row table, which is modeled as a linked list of pages. Each page on that list has slots that are either free
	 * (awaiting the insertion of the location of a free physical row) or available for reallocation requests. An
	 * allocated slot is indicated by a non-zero size field in that slot and the size is the size of the available free
	 * record in bytes.
	 */
	long get(int size) throws IOException {
                //never reclaim used space
                if(appendToEnd) return 0;

                //requested record is bigger than any previously found
                if(lastMaxSize!=-1 && size>lastMaxSize)
                    return 0;

		// Loop through the free physical rowid list until we find
		// a rowid that's large enough.
		long retval = 0;
		PageCursor curs = new PageCursor(_pageman, Magic.FREEPHYSIDS_PAGE);
                int maxSize = -1;
		while (curs.next() != 0) {
			FreePhysicalRowIdPage fp = FreePhysicalRowIdPage.getFreePhysicalRowIdPageView(_file.get(curs.getCurrent()), blockSize);
			int slot = fp.getFirstLargerThan(size);
			if (slot > 0) {
                                //reset maximal size, as record has changed
                                lastMaxSize = -1;
				// got one!
				retval = fp.slotToLocation(slot);

				fp.free(slot);
				if (fp.getCount() == 0) {
					// page became empty - free it
					_file.release(curs.getCurrent(), false);
					_pageman.free(Magic.FREEPHYSIDS_PAGE, curs.getCurrent());
				} else {
					_file.release(curs.getCurrent(), true);
				}

				return retval;
			} else {
                                if(maxSize<-slot)
                                    maxSize=-slot;
				// no luck, go to next page
				_file.release(curs.getCurrent(), false);
			}

		}
                //update maximal size available
                lastMaxSize = maxSize;

		return 0;
	}

	/**
	 * Puts the indicated rowid on the free list, which avaits for commit
	 */
	void put(long rowid, int size) throws IOException {
		freeBlocksInTransactionRowid.add(Long.valueOf(rowid));
		freeBlocksInTransactionSize.add(Integer.valueOf(size));
	}

	public void commit() throws IOException {
		//write all uncommited free records		
		Iterator<Long> rowidIter = freeBlocksInTransactionRowid.iterator();
		Iterator<Integer> sizeIter = freeBlocksInTransactionSize.iterator();
		PageCursor curs = new PageCursor(_pageman, Magic.FREEPHYSIDS_PAGE);
		//iterate over filled pages
		while (curs.next() != 0) {
			long freePage = curs.getCurrent();
			BlockIo curBlock = _file.get(freePage);
			FreePhysicalRowIdPage fp = FreePhysicalRowIdPage.getFreePhysicalRowIdPageView(curBlock, blockSize);
			int slot = fp.getFirstFree();
			//iterate over free slots in page and fill them
			while(slot!=-1 && rowidIter.hasNext()){
				long rowid = rowidIter.next();
				int size = sizeIter.next();		
				short freePhysRowId = fp.alloc(slot);
				fp.setLocationBlock(freePhysRowId, Location.getBlock(rowid));
				fp.setLocationOffset(freePhysRowId, Location.getOffset(rowid));
				fp.FreePhysicalRowId_setSize(freePhysRowId, size);
				slot = fp.getFirstFree();
			}
			_file.release(freePage, true);
			if(!rowidIter.hasNext())
				break;
		}
		
		//now we propably filled all already allocated pages,
		//time to start allocationg new pages
		while(rowidIter.hasNext()){
			//allocate new page
			long freePage = _pageman.allocate(Magic.FREEPHYSIDS_PAGE);
			BlockIo curBlock = _file.get(freePage);
			FreePhysicalRowIdPage fp = FreePhysicalRowIdPage.getFreePhysicalRowIdPageView(curBlock, blockSize);
			int slot = fp.getFirstFree();
			//iterate over free slots in page and fill them
			while(slot!=-1 && rowidIter.hasNext()){
				long rowid = rowidIter.next();
				int size = sizeIter.next();		
				short freePhysRowId = fp.alloc(slot);
				fp.setLocationBlock(freePhysRowId, Location.getBlock(rowid));
				fp.setLocationOffset(freePhysRowId, Location.getOffset(rowid));
				fp.FreePhysicalRowId_setSize(freePhysRowId, size);
				slot = fp.getFirstFree();
			}
			_file.release(freePage, true);
			if(!rowidIter.hasNext())
				break;
		}
		
		if(rowidIter.hasNext())
			throw new InternalError();

		freeBlocksInTransactionRowid.clear();
		freeBlocksInTransactionSize.clear();
		
	}
}
