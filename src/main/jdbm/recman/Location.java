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
 * This class represents a location within a file. Both physical and
 * logical rowids are based on locations internally - this version is
 * used when there is no file block to back the location's data.
 */
final class Location {
	
	static long getBlock(long blockOffset){
		return blockOffset >> 16;
	}
	
	static short getOffset(long blockOffset){
		return (short) (blockOffset & 0xffff);
	}

	
    final private long block;
    final private short offset;
//    private int _hashCode = 0;

    /**
     * Creates a location from a (block, offset) tuple.
     */
    Location(long block, short offset) {
        this.block = block;
        this.offset = offset;
    }

    /**
     * Creates a location from a combined block/offset long, as
     * used in the external representation of logical rowids.
     * 
     * @see #toLong()
     */
    Location(long blockOffset) {
        this.offset = (short) (blockOffset & 0xffff);
        this.block = blockOffset >> 16;
    }

    /**
     * Creates a location based on the data of the physical rowid.
     */
    Location(PhysicalRowId src) {
        block = src.getBlock();
        offset = src.getOffset();
    }

    /**
     * Returns the file block of the location
     */
    long getBlock() {
        return block;
    }

    /**
     * Returns the offset within the block of the location
     */
    short getOffset() {
        return offset;
    }

    /**
     * Returns the external representation of a location when used
     * as a logical rowid, which combines the block and the offset
     * in a single long.
     */
    long toLong() {
        return (block << 16) + (long) offset;
    }

    // overrides of java.lang.Object

    /**
         * Hash code based on the record identifier. This is computed lazily and
         * cached. It supports the buffered record installer, which creates hash
         * collections based on locations.
         * 
         * @see BufferedRecordInstallManager
         */
    public int hashCode() {
    	throw new UnsupportedOperationException();
//        if( _hashCode == 0 ) {
//                _hashCode = new Long(toLong()).hashCode();
//        }
//        return _hashCode;
    }
    
    public boolean equals(Object o) {
        if( this == o ) return true;
//        if (o == null || !(o instanceof Location))
//            return false;
        Location ol = (Location) o;
        return ol.block == block && ol.offset == offset;
    }

    /**
     * True iff the block and offset are both zero.
     */
    public boolean isZero() {
        return block == 0L && offset == (short)0;
    }
    
    public String toString() {
        return "PL(" + block + ":" + offset + ")";
    }
}
