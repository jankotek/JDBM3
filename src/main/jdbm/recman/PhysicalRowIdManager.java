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
 * $Id: PhysicalRowIdManager.java,v 1.3 2003/03/21 03:00:09 boisvert Exp $
 */

package jdbm.recman;

import java.io.IOException;
import java.io.OutputStream;

/**
 * This class manages physical row ids, and their data.
 */
final class PhysicalRowIdManager {

	// The file we're talking to and the associated page manager.
	final private RecordFile file;
	final private PageManager pageman;
	final private FreePhysicalRowIdPageManager freeman;
	final private int BLOCK_SIZE;
	final short DATA_PER_PAGE ;

	/**
	 * Creates a new rowid manager using the indicated record file. and page manager.
	 */
	PhysicalRowIdManager(RecordFile file, PageManager pageManager, FreePhysicalRowIdPageManager freeman) throws IOException {
		this.file = file;
		this.pageman = pageManager;
		this.freeman = freeman;
		this.BLOCK_SIZE = file.BLOCK_SIZE;
		DATA_PER_PAGE = (short) (BLOCK_SIZE - DataPage.O_DATA);
	}

	/**
	 * Inserts a new record. Returns the new physical rowid.
	 */
	long insert(byte[] data, int start, int length) throws IOException {
		if (length < 1)
			throw new IllegalArgumentException("Lenght is <1");
		if (start < 0)
			throw new IllegalArgumentException("negative start");

		long retval = alloc(length);
		write(retval, data, start, length);
		return retval;
	}

	/**
	 * Updates an existing record. Returns the possibly changed physical rowid.
	 */
	long update(long rowid, byte[] data, int start, int length) throws IOException {
		// fetch the record header
		BlockIo block = file.get(Location.getBlock(rowid));
		short head = Location.getOffset(rowid);
		int availSize = RecordHeader.getAvailableSize(block, head);
		if (length > availSize || 
			//difference between free and available space can be only 64KB. 
			//if bigger, need to realocate and free block	
			availSize - length > RecordHeader.MAX_SIZE_SPACE	
		) {
			// not enough space - we need to copy to a new rowid.
			file.release(block);
			free(rowid);
			rowid = alloc(length);
		} else {
			file.release(block);
		}

		// 'nuff space, write it in and return the rowid.
		write(rowid, data, start, length);
		return rowid;
	} 

	/**
	 * Deletes a record.
	 */
	void delete(long rowid) throws IOException {
		free(rowid);
	}

	/**
	 * Retrieves a record.
	 */
	// byte[] fetch( Location rowid )
	// throws IOException
	// {
	// // fetch the record header
	// PageCursor curs = new PageCursor( pageman, rowid.getBlock() );
	// BlockIo block = file.get( curs.getCurrent() );
	// RecordHeader head = new RecordHeader( block, rowid.getOffset() );
	//
	// // allocate a return buffer
	// byte[] retval = new byte[ head.getCurrentSize() ];
	// if ( retval.length == 0 ) {
	// file.release( curs.getCurrent(), false );
	// return retval;
	// }
	//
	// // copy bytes in
	// int offsetInBuffer = 0;
	// int leftToRead = retval.length;
	// short dataOffset = (short) (rowid.getOffset() + RecordHeader.SIZE);
	// while ( leftToRead > 0 ) {
	// // copy current page's data to return buffer
	// int toCopy = RecordFile.BLOCK_SIZE - dataOffset;
	// if ( leftToRead < toCopy ) {
	// toCopy = leftToRead;
	// }
	// System.arraycopy( block.getData(), dataOffset,
	// retval, offsetInBuffer,
	// toCopy );
	//
	// // Go to the next block
	// leftToRead -= toCopy;
	// offsetInBuffer += toCopy;
	//
	// file.release( block );
	//
	// if ( leftToRead > 0 ) {
	// block = file.get( curs.next() );
	// dataOffset = DataPage.O_DATA;
	// }
	//
	// }
	//
	// return retval;
	// }

	void fetch(OutputStream out, long rowid) throws IOException {
		// fetch the record header
		PageCursor curs = new PageCursor(pageman, Location.getBlock(rowid));
		BlockIo block = file.get(curs.getCurrent());
		short head = Location.getOffset(rowid);

		// allocate a return buffer
		// byte[] retval = new byte[ head.getCurrentSize() ];
		final int size = RecordHeader.getCurrentSize(block,head);
		if (size == 0) {
			file.release(curs.getCurrent(), false);
			return;
		}

		// copy bytes in
		int offsetInBuffer = 0;
		int leftToRead = size;
		short dataOffset = (short) (Location.getOffset(rowid) + RecordHeader.SIZE);
		while (leftToRead > 0) {
			// copy current page's data to return buffer
			int toCopy = BLOCK_SIZE - dataOffset;
			if (leftToRead < toCopy) {
				toCopy = leftToRead;
			}
			byte[] blockData = block.getData();
			int finish = dataOffset + toCopy;
			out.write(blockData, dataOffset, finish - dataOffset);

			// Go to the next block
			leftToRead -= toCopy;
			offsetInBuffer += toCopy;
			// out.flush();
			file.release(block);

			if (leftToRead > 0) {
				block = file.get(curs.next());
				dataOffset = DataPage.O_DATA;
			}

		}

		// return retval;
	}

	/**
	 * Allocate a new rowid with the indicated size.
	 */
	private long alloc(int size) throws IOException {
		size = RecordHeader.roundAvailableSize(size);
		long retval = freeman.get(size);
		if (retval == 0) {
			retval = allocNew(size, pageman.getLast(Magic.USED_PAGE));
		}
		return retval;
	}

	/**
	 * Allocates a new rowid. The second parameter is there to allow for a recursive call - it indicates where the
	 * search should start.
	 */
	private long allocNew(int size, long start) throws IOException {
		BlockIo curBlock;
		DataPage curPage;
		if (start == 0) {
			// we need to create a new page.
			start = pageman.allocate(Magic.USED_PAGE);
			curBlock = file.get(start);
			curPage = DataPage.getDataPageView(curBlock,BLOCK_SIZE);
			curPage.setFirst(DataPage.O_DATA);
			RecordHeader.setAvailableSize(curBlock, DataPage.O_DATA, 0);
			RecordHeader.setCurrentSize(curBlock, DataPage.O_DATA, 0);
		} else {
			curBlock = file.get(start);
			curPage = DataPage.getDataPageView(curBlock,BLOCK_SIZE);
		}

		// follow the rowids on this page to get to the last one. We don't
		// fall off, because this is the last page, remember?
		short pos = curPage.getFirst();
		if (pos == 0) {
			// page is exactly filled by the last block of a record
			file.release(curBlock);
			return allocNew(size, 0);
		}

		short hdr = pos;
		while (RecordHeader.getAvailableSize(curBlock, hdr) != 0 && pos < BLOCK_SIZE) {
			pos += RecordHeader.getAvailableSize(curBlock, hdr) + RecordHeader.SIZE;
			if (pos == BLOCK_SIZE) {
				// Again, a filled page.
				file.release(curBlock);
				return allocNew(size, 0);
			}

			hdr = pos;
		}

		if (pos == RecordHeader.SIZE) {
			// the last record exactly filled the page. Restart forcing
			// a new page.
			file.release(curBlock);
		}

		// we have the position, now tack on extra pages until we've got
		// enough space.
		long retval = Location.toLong(start, pos);
		int freeHere = BLOCK_SIZE - pos - RecordHeader.SIZE;
		if (freeHere < size) {
			// check whether the last page would have only a small bit left.
			// if yes, increase the allocation. A small bit is a record
			// header plus 16 bytes.
			int lastSize = (size - freeHere) % DATA_PER_PAGE;
			if ((DATA_PER_PAGE - lastSize) < (RecordHeader.SIZE + 16)) {
				size += (DATA_PER_PAGE - lastSize);
				size = RecordHeader.roundAvailableSize(size);
			}

			// write out the header now so we don't have to come back.
			RecordHeader.setAvailableSize(curBlock, hdr, size);
			file.release(start, true);

			int neededLeft = size - freeHere;
			// Refactor these two blocks!
			while (neededLeft >= DATA_PER_PAGE) {
				start = pageman.allocate(Magic.USED_PAGE);
				curBlock = file.get(start);
				curPage = DataPage.getDataPageView(curBlock, BLOCK_SIZE);
				curPage.setFirst((short) 0); // no rowids, just data
				file.release(start, true);
				neededLeft -= DATA_PER_PAGE;
			}
			if (neededLeft > 0) {
				// done with whole chunks, allocate last fragment.
				start = pageman.allocate(Magic.USED_PAGE);
				curBlock = file.get(start);
				curPage = DataPage.getDataPageView(curBlock, BLOCK_SIZE);
				curPage.setFirst((short) (DataPage.O_DATA + neededLeft));
				file.release(start, true);
			}
		} else {
			// just update the current page. If there's less than 16 bytes
			// left, we increase the allocation (16 bytes is an arbitrary
			// number).
			if (freeHere - size <= (16 + RecordHeader.SIZE)) {
				size = freeHere;
			}
			RecordHeader.setAvailableSize(curBlock, hdr, size);
			file.release(start, true);
		}
		return retval;

	}

	private void free(long id) throws IOException {
		// get the rowid, and write a zero current size into it.
		BlockIo curBlock = file.get(Location.getBlock(id));
		DataPage curPage = DataPage.getDataPageView(curBlock,BLOCK_SIZE);

		RecordHeader.setCurrentSize(curBlock, Location.getOffset(id), 0);
		file.release(Location.getBlock(id), true);

		// write the rowid to the free list
		freeman.put(id, RecordHeader.getAvailableSize(curBlock, Location.getOffset(id)));	
	}

	/**
	 * Writes out data to a rowid. Assumes that any resizing has been done.
	 */
	private void write(long rowid, byte[] data, int start, int length) throws IOException {
		PageCursor curs = new PageCursor(pageman, Location.getBlock(rowid));
		BlockIo block = file.get(curs.getCurrent());
		short hdr = Location.getOffset(rowid);
		RecordHeader.setCurrentSize(block, hdr, length);
		if (length == 0) {
			file.release(curs.getCurrent(), true);
			return;
		}

		// copy bytes in
		int offsetInBuffer = start;
		int leftToWrite = length;
		short dataOffset = (short) (Location.getOffset(rowid) + RecordHeader.SIZE);
		while (leftToWrite > 0) {
			// copy current page's data to return buffer
			int toCopy = BLOCK_SIZE - dataOffset;

			if (leftToWrite < toCopy) {
				toCopy = leftToWrite;
			}
			System.arraycopy(data, offsetInBuffer, block.getData(), dataOffset, toCopy);

			// Go to the next block
			leftToWrite -= toCopy;
			offsetInBuffer += toCopy;

			file.release(curs.getCurrent(), true);

			if (leftToWrite > 0) {
				block = file.get(curs.next());
				dataOffset = DataPage.O_DATA;
			}
		}
	}

	void commit() throws IOException {
		freeman.commit();		
	}
}
