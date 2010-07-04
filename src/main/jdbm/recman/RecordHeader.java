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
 *  The data that comes at the start of a record of data. It stores 
 *  both the current size and the avaliable size for the record - the latter
 *  can be bigger than the former, which allows the record to grow without
 *  needing to be moved and which allows the system to put small records
 *  in larger free spots.
 */
final class RecordHeader {
    // offsets
    private static final short O_CURRENTSIZE = 0; // int currentSize
    private static final short O_AVAILABLESIZE = Magic.SZ_UNSIGNED_SHORT; // int availableSize
    static final int SIZE = O_AVAILABLESIZE + Magic.SZ_UNSIGNED_SHORT;
    
    /**
     * Maximal differnece between current and available size,
     * Maximal value is resorved for currentSize 0, so use -1
     */
    static final int MAX_SIZE_SPACE = BlockIo.UNSIGNED_SHORT_MAX -1;
    
    // my block and the position within the block
    final private BlockIo block;
    final private short pos;

    /**
     *  Constructs a record header from the indicated data starting at
     *  the indicated position.
     */
    RecordHeader(BlockIo block, short pos) {
        this.block = block;
        this.pos = pos;
        if (pos > (RecordFile.BLOCK_SIZE - SIZE))
            throw new Error("Offset too large for record header (" 
                            + block.getBlockId() + ":" 
                            + pos + ")");
    }

    /** Returns the current size */
    int getCurrentSize() {
    	int s = block.readUnsignedshort(pos + O_CURRENTSIZE);
    	if(s == BlockIo.UNSIGNED_SHORT_MAX)
    		return 0;
        return getAvailableSize() - s;
    }
    
    /** Sets the current size */
    void setCurrentSize(int value) {
    	if(value == 0){
    		block.writeUnsignedShort(pos + O_CURRENTSIZE, BlockIo.UNSIGNED_SHORT_MAX);
    		return;
    	}
        int availSize = getAvailableSize();
        if(value < (availSize - MAX_SIZE_SPACE) || value>availSize)
        	throw new IllegalArgumentException("currentSize out of bounds, need to realocate "+value+ " - "+availSize);
    	block.writeUnsignedShort(pos + O_CURRENTSIZE, availSize - value);
    }
    
    /** Returns the available size */
    int getAvailableSize() {
        int val  = block.readUnsignedshort(pos + O_AVAILABLESIZE);
        int multiplier = val & sizeMask;
        int counter = val - multiplier;
        switch (multiplier){
        	case 0<<14: return counter * multi0;
        	case 1<<14: return counter * multi1;
        	case 2<<14: return counter * multi2;
        	case 3<<14: return counter * multi3;
        	default: throw new InternalError();
        }
    }
    
    /** Sets the available size */
    void setAvailableSize(int value) {    	
    	//TODO remove assertion in production code
//    	if(value != roundAvailableSize(value))
//    		throw new IllegalArgumentException("value is not rounded");
    	int oldCurrSize = getCurrentSize();
    	int multiplyer = 0;
    	int counter = 0;
    	if(value<base1){
    		multiplyer = 0;
    		counter = value / multi0;
    	}else if(value<base2){
    		multiplyer = 1 <<14;
    		counter = value/multi1;    		
    	}else if(value<base3){
    		multiplyer = 2 <<14;
    		counter = value/multi2;
    	}else{
    		multiplyer = 3 <<14;
    		counter = value/multi3;
    	}
    	if(counter>(1<<14))
    		throw new InternalError(""+value);
        	
        block.writeUnsignedShort(pos + O_AVAILABLESIZE, multiplyer | counter);
        setCurrentSize(oldCurrSize);
    }
    
    static final int sizeMask = 1<<15 | 1<<14;
    static final int multi0 = 1;
    static final int multi1 = 1<<2;
    static final int multi2 = 1<<8;
    //is divided by not to make sure currSize does not overflow at multiplier rounding
    static final int multi3 = MAX_SIZE_SPACE/2 -100;  

    
    static final int base0 = 0;
    static final int base1 = base0 + multi0 * (1<<14-1);
    static final int base2 = base0 + multi1 * (1<<14-1);
    static final int base3 = base0 + multi2 * (1<<14-1);
    static final int MAX_RECORD_SIZE = base0 + multi3 * (1<<14-1);
    
    
    
    static int roundAvailableSize(int value){
    	if(value<base1)
    		return value;
    	else if(value<base2)
    		return value + (value%multi1==0?0:(multi1 - value%multi1));
    	else if(value<base3)
    		return value + (value%multi2==0?0:(multi2 - value%multi2));
    	else if(value<MAX_RECORD_SIZE)
    		return value + (value%multi3==0?0:(multi3 - value%multi3));
    	throw 
    		new InternalError("Maximal record size ("+MAX_RECORD_SIZE+") exceeded: "+value);
    }


    public String toString() {
        return "RH(" + block.getBlockId() + ":" + pos 
            + ", avl=" + getAvailableSize()
            + ", cur=" + getCurrentSize() 
            + ")";
    }
}
