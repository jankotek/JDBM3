///*******************************************************************************
// * Copyright 2010 Cees De Groot, Alex Boisvert, Jan Kotek
// * 
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// * 
// *   http://www.apache.org/licenses/LICENSE-2.0
// * 
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// ******************************************************************************/
//
//
//package jdbm.recman;
//
///**
// * This class extends the physical rowid with a size value to indicated the size 
// * of a free rowid on the free rowid list.
// */
//final class FreePhysicalRowId extends PhysicalRowId {
//	// offsets
//	static final short O_SIZE = PhysicalRowId.SIZE; // int size
//	static final short SIZE = O_SIZE + Magic.SZ_INT;
//	
//	/**
//	 * Used to cache size value. So no read is necessary when looking for free block 
//	 */
//	private int size = -1; //FIXME need to check if this RowId can be accessed from somewhere else
//
//	/**
//	 * Constructs a physical rowid from the indicated data starting 
//	 * at the indicated position.
//	 */
//	FreePhysicalRowId(BlockIo block, short pos) {
//		super(block, pos);
//	}
//
//	/** Returns the size */
//	int getSize() {			
//		return size = block.readInt(pos + O_SIZE);
//	}
//
//	/** Sets the size */
//	void setSize(int value) {
//		block.writeInt(pos + O_SIZE, value);
//	}
//
//}
