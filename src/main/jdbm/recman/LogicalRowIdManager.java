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
 * This class manages the linked lists of logical rowid pages.
 */
final class LogicalRowIdManager {
	// our record file and associated page manager
	private final RecordFile file;
	private final PageManager pageman;
	private final FreeLogicalRowIdPageManager freeman;
	final short ELEMS_PER_PAGE;
	private int blockSize; 

	/**
	 * Creates a log rowid manager using the indicated record file and page manager
	 */
	LogicalRowIdManager(RecordFile file, PageManager pageman,FreeLogicalRowIdPageManager freeman) throws IOException {
		this.file = file;
		this.pageman = pageman;
		this.freeman = freeman;
		this.blockSize = file.BLOCK_SIZE;
		this.ELEMS_PER_PAGE = (short)((blockSize - TranslationPage.O_TRANS) / TranslationPage.PhysicalRowId_SIZE);
	}

	/**
	 * Creates a new logical rowid pointing to the indicated physical id
	 */
	long insert(long loc) throws IOException {
		// check whether there's a free rowid to reuse
		long retval = freeman.get();
		if (retval == 0) {
			// no. This means that we bootstrap things by allocating
			// a new translation page and freeing all the rowids on it.
			long firstPage = pageman.allocate(Magic.TRANSLATION_PAGE);
			short curOffset = TranslationPage.O_TRANS;
			for (int i = 0; i < ELEMS_PER_PAGE; i++) {
				freeman.put(Location.toLong(firstPage, curOffset));
				curOffset += PageHeader.PhysicalRowId_SIZE;
			}
			
			retval = freeman.get();
			if (retval == 0) {
				throw new Error("couldn't obtain free translation");
			}
		}
		// write the translation.
		update(retval, loc);
		return retval;
	}
	
	/**
	 * Insert at forced location, use only for defragmentation !!
	 * @param logicalRowId
	 * @param physLoc
	 * @throws IOException 
	 */
	void forceInsert(long logicalRowId, long physLoc) throws IOException {
		//create pages until we reach requested block
		long lastBlock  = pageman.getLast(Magic.TRANSLATION_PAGE);
		while(lastBlock!=Location.getBlock(logicalRowId)){
			lastBlock = pageman.allocate(Magic.TRANSLATION_PAGE);
			if(lastBlock>Location.getBlock(logicalRowId))
				throw new Error("outallocated");
		}
		if(fetch(logicalRowId) != 0)
			throw new Error("can not forceInsert, record already exists: "+logicalRowId);		
		
		update(logicalRowId, physLoc);
	}


	/**
	 * Releases the indicated logical rowid.
	 */
	void delete(long rowid) throws IOException {
		//zero out old location, is needed for defragmentation
		TranslationPage xlatPage = TranslationPage.getTranslationPageView(file.get(Location.getBlock(rowid)),blockSize);
		xlatPage.setLocationBlock(Location.getOffset(rowid), 0);
		xlatPage.setLocationOffset(Location.getOffset(rowid), (short)0);
		file.release(Location.getBlock(rowid), true);
		freeman.put(rowid);
	}

	/**
	 * Updates the mapping
	 * 
	 * @param rowid
	 *            The logical rowid
	 * @param loc
	 *            The physical rowid
	 */
	void update(long rowid, long loc) throws IOException {

		TranslationPage xlatPage = TranslationPage.getTranslationPageView(file.get(Location.getBlock(rowid)),blockSize);
		//make sure it is right type of page
		
		
//		PhysicalRowId physid = xlatPage.get(rowid.getOffset());
//		physid.setBlock(loc.getBlock());
//		physid.setOffset(loc.getOffset());
		xlatPage.setLocationBlock(Location.getOffset(rowid), Location.getBlock(loc));
		xlatPage.setLocationOffset(Location.getOffset(rowid), Location.getOffset(loc));
		file.release(Location.getBlock(rowid), true);
	}

	/**
	 * Returns a mapping
	 * 
	 * @param rowid
	 *            The logical rowid
	 * @return The physical rowid, 0 if does not exist
	 */
	long fetch(long rowid) throws IOException {
		final long block = Location.getBlock(rowid);
		long last = pageman.getLast(Magic.TRANSLATION_PAGE);
		if(last+1<block)
			return 0;
		
		final short offset = Location.getOffset(rowid);
		
		BlockIo bio = file.get(block);
		TranslationPage xlatPage = TranslationPage.getTranslationPageView(bio,blockSize);		
		try {
			long retval = Location.toLong(
					xlatPage.getLocationBlock(offset),
					xlatPage.getLocationOffset(offset));
			return retval;
		} finally {
			file.release(block, false);
		}
	}

	void commit() throws IOException{
		freeman.commit();
	}

}
