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
 * Class describing a page that holds data.
 */
final class DataPage extends PageHeader {
	// offsets
	private static final short O_FIRST = PageHeader.SIZE; // short firstrowid
	static final short O_DATA = (short) (O_FIRST + Magic.SZ_SHORT);
	final short DATA_PER_PAGE ;

	/**
	 * Constructs a data page view from the indicated block.
	 */
	DataPage(BlockIo block, int blockSize) {
		super(block);
		DATA_PER_PAGE = (short) (blockSize - O_DATA);
	}

	/**
	 * Factory method to create or return a data page for the indicated block.
	 */
	static DataPage getDataPageView(BlockIo block, int blockSize) {
		BlockView view = block.getView();
		if (view != null && view instanceof DataPage)
			return (DataPage) view;
		else
			return new DataPage(block, blockSize);
	}

	/** Returns the first rowid's offset */
	short getFirst() {
		return block.readShort(O_FIRST);
	}

	/** Sets the first rowid's offset */
	void setFirst(short value) {
		paranoiaMagicOk();
		if (value > 0 && value < O_DATA)
			throw new Error("DataPage.setFirst: offset " + value + " too small");
		block.writeShort(O_FIRST, value);
	}
}
