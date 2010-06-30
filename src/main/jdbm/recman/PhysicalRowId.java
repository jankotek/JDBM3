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
// *  A physical rowid is nothing else than a pointer to a physical location
// *  in a file - a (block, offset) tuple.
// *  <P>
// *  <B>Note</B>: The fact that the offset is modelled as a short limits 
// *  the block size to 32k.
// */
//class PhysicalRowId {
//    // offsets
//    static final short O_BLOCK = 0; // long block
//    static final short O_OFFSET = Magic.SZ_LONG; // short offset
//    static final int SIZE = O_OFFSET + Magic.SZ_SHORT;
//    
//    // my block and the position within the block
//    BlockIo block;
//    short pos;
//
//    /**
//     *  Constructs a physical rowid from the indicated data starting at
//     *  the indicated position.
//     */
//    PhysicalRowId(BlockIo block, short pos) {
//        this.block = block;
//        this.pos = pos;
//    }
//    
//    /** Returns the block number */
//    long getBlock() {
//        return block.readLong(pos + O_BLOCK);
//    }
//    
//    /** Sets the block number */
//    void setBlock(long value) {
//        block.writeLong(pos + O_BLOCK, value);
//    }
//    
//    /** Returns the offset */
//    short getOffset() {
//        return block.readShort(pos + O_OFFSET);
//    }
//    
//    /** Sets the offset */
//    void setOffset(short value) {
//        block.writeShort(pos + O_OFFSET, value);
//    }
//}
