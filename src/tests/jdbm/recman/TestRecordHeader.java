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

import java.util.Random;

import junit.framework.TestCase;

/**
 * This class contains all Unit tests for {@link RecordHeader}.
 */
public class TestRecordHeader extends TestCase {

	public TestRecordHeader(String name) {
		super(name);
	}

	/**
	 * Test basics - read and write at an offset
	 */
	public void testReadWrite() throws Exception {
		byte[] data = new byte[RecordFile.BLOCK_SIZE];
		BlockIo test = new BlockIo(0, data);
		RecordHeader hdr = new RecordHeader(test, (short) 6);
		hdr.setAvailableSize(2345);
		hdr.setCurrentSize(1000);

		assertEquals("current size", 1000, hdr.getCurrentSize());
		assertEquals("available size", 2345, hdr.getAvailableSize());
	}
	
	public void testRecordSize(){
		//System.out.println("size "+RecordHeader.roundAvailableSize(100000));
		
		byte[] data = new byte[RecordFile.BLOCK_SIZE];
		BlockIo test = new BlockIo(0, data);
		Random r = new Random();
		RecordHeader hdr = new RecordHeader(test, (short) 6);
		double size = 2;
		while(size<RecordHeader.MAX_RECORD_SIZE){
			//set size
			int currSize = (int) size;
			int availSize = RecordHeader.roundAvailableSize(currSize);
			 
			assertTrue(availSize - currSize<RecordHeader.MAX_SIZE_SPACE);
			assertTrue(currSize<=availSize);

			//make sure it writes and reads back correctly
			hdr.setAvailableSize(availSize);
			hdr.setCurrentSize(currSize);

			assertEquals("current size", currSize, hdr.getCurrentSize());
			assertEquals("available size", availSize, hdr.getAvailableSize());
			
			//try random size within given offset
			int newCurrSize = availSize - r.nextInt(RecordHeader.MAX_SIZE_SPACE);
			if(newCurrSize<0) newCurrSize = 0;
			hdr.setCurrentSize(newCurrSize);
			assertEquals("current size", newCurrSize, hdr.getCurrentSize());
			
			hdr.setCurrentSize(0);
			size = size * 1.1;
		}

	}

}
