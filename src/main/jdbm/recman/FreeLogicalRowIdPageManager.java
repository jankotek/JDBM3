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
import java.util.List;

/**
 *  This class manages free Logical rowid pages and provides methods
 *  to free and allocate Logical rowids on a high level.
 */
final class FreeLogicalRowIdPageManager {
    // our record file
    private RecordFile file;
    // our page manager
    private PageManager pageman;
	private int blockSize;
	
	final List<Long> freeBlocksInTransactionRowid = new ArrayList<Long>();


    /**
     *  Creates a new instance using the indicated record file and
     *  page manager.
     */
    FreeLogicalRowIdPageManager(RecordFile file,
                                PageManager pageman) throws IOException {
        this.file = file;
        this.pageman = pageman;
        this.blockSize = file.BLOCK_SIZE;
    }

    /**
     *  Returns a free Logical rowid, or
     *  0 if nothing was found.
     */
    long get() throws IOException {
    	if(!freeBlocksInTransactionRowid.isEmpty()){
    		long first = freeBlocksInTransactionRowid.get(freeBlocksInTransactionRowid.size()-1);
    		freeBlocksInTransactionRowid.remove(freeBlocksInTransactionRowid.size()-1);
    		return first;
    	}
  
        // Loop through the free Logical rowid list until we find
        // the first rowid.
        long retval = 0;
        PageCursor curs = new PageCursor(pageman, Magic.FREELOGIDS_PAGE);
        while (curs.next() != 0) {
            FreeLogicalRowIdPage fp = FreeLogicalRowIdPage
                .getFreeLogicalRowIdPageView(file.get(curs.getCurrent()),blockSize);
            int slot = fp.getFirstAllocated();
            if (slot != -1) {
                // got one!
                retval = fp.slotToLocation(slot);
                    
                fp.free(slot);
                if (fp.getCount() == 0) {
                    // page became empty - free it
                    file.release(curs.getCurrent(), false);
                    pageman.free(Magic.FREELOGIDS_PAGE, curs.getCurrent());
                }
                else
                    file.release(curs.getCurrent(), true);
                
                return retval;
            }
            else {
                // no luck, go to next page
                file.release(curs.getCurrent(), false);
            }     
        }
        return 0;
    }

    /**
     *  Puts the indicated rowid on the free list
     */
    void put(long rowid)throws IOException {
        freeBlocksInTransactionRowid.add(Long.valueOf(rowid));
    }
    

	public void commit() throws IOException {
		//write all uncommited free records		
		Iterator<Long> rowidIter = freeBlocksInTransactionRowid.iterator();
		PageCursor curs = new PageCursor(pageman, Magic.FREELOGIDS_PAGE);
		//iterate over filled pages
		while (curs.next() != 0) {
			long freePage = curs.getCurrent();
			BlockIo curBlock = file.get(freePage);
			FreeLogicalRowIdPage fp = FreeLogicalRowIdPage.getFreeLogicalRowIdPageView(curBlock, blockSize);
			int slot = fp.getFirstFree();
			//iterate over free slots in page and fill them
			while(slot!=-1 && rowidIter.hasNext()){
				long rowid = rowidIter.next();		
				short freePhysRowId = fp.alloc(slot);
				fp.setLocationBlock(freePhysRowId, Location.getBlock(rowid));
				fp.setLocationOffset(freePhysRowId, Location.getOffset(rowid));			
				slot = fp.getFirstFree();
			}
			file.release(freePage, true);
			if(!rowidIter.hasNext())
				break;
		}
		
		//now we propably filled all already allocated pages,
		//time to start allocationg new pages
		while(rowidIter.hasNext()){
			//allocate new page
			long freePage = pageman.allocate(Magic.FREELOGIDS_PAGE);
			BlockIo curBlock = file.get(freePage);
			FreeLogicalRowIdPage fp = FreeLogicalRowIdPage.getFreeLogicalRowIdPageView(curBlock, blockSize);
			int slot = fp.getFirstFree();
			//iterate over free slots in page and fill them
			while(slot!=-1 && rowidIter.hasNext()){
				long rowid = rowidIter.next();		
				short freePhysRowId = fp.alloc(slot);
				fp.setLocationBlock(freePhysRowId, Location.getBlock(rowid));
				fp.setLocationOffset(freePhysRowId, Location.getOffset(rowid));
				slot = fp.getFirstFree();
			}
			file.release(freePage, true);
			if(!rowidIter.hasNext())
				break;
		}
		
		if(rowidIter.hasNext())
			throw new InternalError();

		freeBlocksInTransactionRowid.clear();
		
	}
}
