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

package net.kotek.jdbm;

import junit.framework.TestCase;

/**
 * This class contains all Unit tests for {@link Location}.
 */
public class LocationTest extends TestCase {

    public LocationTest(String name) {
        super(name);
    }

    /**
     * Basic tests
     */
    public void testBasics() {

        long loc = Location.toLong(10, (short) 20);

        assertEquals("block2", 10, Location.getBlock(loc));
        assertEquals("offset2", 20, Location.getOffset(loc));

    }
    
    
    public void testShift(){
        int shift = 0xFF0000FF;
        assertEquals( shift>>>24, 0xFF);
        assertEquals( shift<<8, 0xFF00);

                     
    }
    
    
    public void testCompressRecid(){
        for(long l = Magic.PAGE_HEADER_SIZE;l<Storage.BLOCK_SIZE;l+=6){
            assertEquals(l,Location.decompressRecid(Location.compressRecid(l)));
        }
        
        for(long l = Magic.PAGE_HEADER_SIZE+Storage.BLOCK_SIZE*5;l<Storage.BLOCK_SIZE*6;l+=6){
            assertEquals(l,Location.decompressRecid(Location.compressRecid(l)));
        }
        
    }

}
