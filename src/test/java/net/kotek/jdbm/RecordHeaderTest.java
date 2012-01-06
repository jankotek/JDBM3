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

import java.util.Random;

import junit.framework.TestCase;

/**
 * This class contains all Unit tests for {@link RecordHeader}.
 */
public class RecordHeaderTest extends TestCase {


    /**
     * Test basics - read and write at an offset
     */
    public void testReadWrite() throws Exception {
        byte[] data = new byte[Storage.BLOCK_SIZE];
        BlockIo test = new BlockIo(0, data);
        //RecordHeader hdr = new RecordHeader(test, (short) 6);
        RecordHeader.setAvailableSize(test, (short) 6, 2345);
        RecordHeader.setCurrentSize(test, (short) 6, 2300);

        assertEquals("current size", 2300, RecordHeader.getCurrentSize(test, (short) 6));
        assertEquals("available size", 2345, RecordHeader.getAvailableSize(test, (short) 6));
    }

    public void testRecordSize() {

        System.out.println("MAX_RECORD_SIZE " + RecordHeader.MAX_RECORD_SIZE);

        assertEquals("inconsistent rounding at max rec size",
                RecordHeader.MAX_RECORD_SIZE, RecordHeader.roundAvailableSize(RecordHeader.MAX_RECORD_SIZE));

        byte[] data = new byte[Storage.BLOCK_SIZE];
        BlockIo test = new BlockIo(0, data);
        Random r = new Random();
        //RecordHeader hdr = new RecordHeader(test, (short) 6);

        for (int size = 2; size <= RecordHeader.MAX_RECORD_SIZE; size++) {
            //set size
            int currSize = size;
            int availSize = RecordHeader.roundAvailableSize(currSize);

            assertTrue(availSize - currSize < RecordHeader.MAX_SIZE_SPACE);
            assertTrue(currSize <= availSize);

            assertEquals("size rounding function does not provide consistent results " + availSize,
                    availSize, RecordHeader.roundAvailableSize(availSize));

            //make sure it writes and reads back correctly
            RecordHeader.setAvailableSize(test, (short) 6, availSize);
            assertEquals("available size", availSize, RecordHeader.getAvailableSize(test, (short) 6));
            RecordHeader.setCurrentSize(test, (short) 6, currSize);

            assertEquals("current size", currSize, RecordHeader.getCurrentSize(test, (short) 6));


            //try random size within given offset
            int newCurrSize = availSize - r.nextInt(RecordHeader.MAX_SIZE_SPACE);
            if (newCurrSize < 0) newCurrSize = 0;
            RecordHeader.setCurrentSize(test, (short) 6, newCurrSize);
            assertEquals("current size", newCurrSize, RecordHeader.getCurrentSize(test, (short) 6));

            RecordHeader.setCurrentSize(test, (short) 6, 0);

            size++;

            // comment out next line to do full test
            if (size > 1e6)
                size = (int) (size * 1.01);
        }

    }

    public void testMaxRecordSize() {

        long max = 0;
        for (int i = 0; i < 1e7; i++) {
            int deconverted = RecordHeader.deconvertAvailSize(RecordHeader.convertAvailSize(i));
            if (i == deconverted) {
                max = i;
            }
        }
        assertEquals("Maximal record size does not match the calculated one: " + max, max, RecordHeader.MAX_RECORD_SIZE);

    }

    public void testRoundingSmall() {
        for (int i = 0; i <= Short.MAX_VALUE; i++) {
            assertEquals(i, RecordHeader.convertAvailSize(i));
        }
    }

    public void testRounding() {

        for (int i = 0; i < RecordHeader.MAX_RECORD_SIZE; i++) {
            int deconverted = RecordHeader.deconvertAvailSize(RecordHeader.convertAvailSize(i));
            assertTrue("deconverted size is smaller than actual: " + i + " versus " + deconverted, deconverted >= i);
        }

    }


    public void testSetCurrentSize() {
        BlockIo b = new BlockIo(4l, new byte[Storage.BLOCK_SIZE]);
        short pos = 10;

        RecordHeader.setAvailableSize(b, pos, 1000);
        assertEquals(1000, RecordHeader.getAvailableSize(b, pos));
        RecordHeader.setCurrentSize(b, pos, 900);
        assertEquals(900, RecordHeader.getCurrentSize(b, pos));
        RecordHeader.setCurrentSize(b, pos, 0);
        assertEquals(0, RecordHeader.getCurrentSize(b, pos));
        RecordHeader.setCurrentSize(b, pos, 1000 - 254);
        assertEquals(1000 - 254, RecordHeader.getCurrentSize(b, pos));

        short pos2 = 20;
        RecordHeader.setAvailableSize(b, pos2, 10000);
        assertEquals(10000, RecordHeader.getAvailableSize(b, pos2));
        RecordHeader.setCurrentSize(b, pos2, 10000);
        assertEquals(10000, RecordHeader.getCurrentSize(b, pos2));

    }

}
