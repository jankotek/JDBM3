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


package jdbm;

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
    private static final short O_AVAILABLESIZE = Magic.SZ_SHORT; // int availableSize
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
        return deconvertAvailSize(block.readShort(pos + O_AVAILABLESIZE));
    }
    
    /** Sets the available size */
    static void setAvailableSize(final BlockIo block, final short pos,int value) {
    	if(value != roundAvailableSize(value))
    		throw new IllegalArgumentException("value is not rounded");
    	int oldCurrSize = getCurrentSize(block,pos);

        block.writeShort(pos + O_AVAILABLESIZE, convertAvailSize(value));
        setCurrentSize(block,pos,oldCurrSize);
    }


    private static short convertAvailSize(final int recordSize){
        if(recordSize<=Short.MAX_VALUE)
            return (short) recordSize;
        else{
            int shift = recordSize - Short.MAX_VALUE;
            shift = - shift;
            return (short) (shift>>8);
        }

    }
    
    private static int deconvertAvailSize(short converted){
        if(converted>=0)
            return converted;
        else{
            int shifted = converted<<8;
            shifted = - shifted;
            return Short.MAX_VALUE + shifted;
        }

    }
    

    static final int MAX_RECORD_SIZE = 8421375;


    
    static int roundAvailableSize(int value){
    	if(value>MAX_RECORD_SIZE)
    		new InternalError("Maximal record size ("+MAX_RECORD_SIZE+") exceeded: "+value);
    	return deconvertAvailSize(convertAvailSize(value));
    }


//   this code is used to find MAXIMAL_RECORD_SIZE
//    public static void main(String[] args){
//        long max = 0;
//        for(int i=0;i<1e7;i++){
//            if(i == deconvertAvailSize(convertAvailSize(i))){
//                max = i;
//            }
//        }
//        System.out.println(max);
//    }
//

}
