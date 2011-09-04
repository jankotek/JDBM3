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
 *  <p/>
 *  In JDBM 1.0 both values were stored as four-byte integers. This was very wastefull.
 *  Now available size is stored in two bytes, it is compressed, so maximal value is up to 120 MB (not sure with exact number)
 *  Current size is stored as two-byte-unsigned-short difference from Available Size.
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
    

    /** Returns the current size */
    static int getCurrentSize(final BlockIo block, final short pos) {
    	int s = block.readUnsignedshort(pos + O_CURRENTSIZE);
    	if(s == BlockIo.UNSIGNED_SHORT_MAX)
    		return 0;
        return getAvailableSize(block, pos) - s;
    }
    
    /** Sets the current size */
    static void setCurrentSize(final BlockIo block, final short pos,int value) {
    	if(value == 0){
    		block.writeUnsignedShort(pos + O_CURRENTSIZE, BlockIo.UNSIGNED_SHORT_MAX);
    		return;
    	}
        int availSize = getAvailableSize(block,pos);
        if(value < (availSize - MAX_SIZE_SPACE) || value>availSize)
        	throw new IllegalArgumentException("currentSize out of bounds, need to realocate "+value+ " - "+availSize);
    	block.writeUnsignedShort(pos + O_CURRENTSIZE, availSize - value);
    }
    
    /** Returns the available size */
    static int getAvailableSize(final BlockIo block, final short pos) {
        int val  = block.readUnsignedshort(pos + O_AVAILABLESIZE);
        return deconvertAvailSize(val);
    }
    
    /** Sets the available size */
    static void setAvailableSize(final BlockIo block, final short pos,int value) {
//    	if(value != roundAvailableSize(value))
//    		throw new IllegalArgumentException("value is not rounded");
    	int oldCurrSize = getCurrentSize(block,pos);

        block.writeUnsignedShort(pos + O_AVAILABLESIZE, convertAvailSize(value));
        setCurrentSize(block,pos,oldCurrSize);
    }


    private static int convertAvailSize(final int recordSize){
    	int multiplyer = 0;
    	int counter = 0;
    	if(recordSize<=base1){
    		multiplyer = 0;
    		counter = recordSize / multi0;
    	}else if(recordSize<base2){
    		multiplyer = 1 <<14;
    		int val2 = recordSize -base1;
    		counter = val2/multi1;
    		if(val2 %multi1 != 0)
    			counter++;
    	}else if(recordSize<base3){
    		multiplyer = 2 <<14;
    		int val2 = recordSize -base2;
    		counter = val2/multi2;
    		if(val2 %multi2 != 0)
    			counter++;
    	}else{
    		multiplyer = 3 <<14;
    		int val2 = recordSize -base3;
    		counter = val2/multi3;
    		if(val2 %multi3 != 0)
    			counter++;    	
    	}
    	if(counter>=(1<<14))
    		throw new InternalError(""+recordSize);
    	
    	return  multiplyer + counter;
    }
    
    private static int deconvertAvailSize(int recordSize){
        int multiplier = (recordSize & sizeMask) >>14;
        int counter = recordSize - (recordSize &sizeMask);
        switch (multiplier){
        	case 0: return counter * multi0;
        	case 1: return base1 + counter * multi1;
        	case 2: return base2 + counter * multi2;
        	case 3: return base3 + counter * multi3;
        	default: throw new InternalError("error deconverting: "+recordSize);
        }
    }
    
    static final int sizeMask = 1<<15 | 1<<14;
    static final int multi0 = 1;
    static final int multi1 = 1<<4;
    static final int multi2 = 1<<8;
    static final int multi3 = 1<< 13;  

    
    static final int base0 = 0;
    static final int base1 = base0 + multi0 * ((1<<14)-2);
    static final int base2 = base1 + multi1 * ((1<<14)-2);
    static final int base3 = base2 + multi2 * ((1<<14)-2);
    static final int base4 = base3 + multi3 * (1<<14)-2;
    
    static final int MAX_RECORD_SIZE = roundAvailableSize(base4 - multi3 * 100);
    
    
    
    static int roundAvailableSize(int value){
    	if(value>MAX_RECORD_SIZE && MAX_RECORD_SIZE!=0)    		
    		new InternalError("Maximal record size ("+MAX_RECORD_SIZE+") exceeded: "+value);
    	return deconvertAvailSize(convertAvailSize(value));
    }
    


}
