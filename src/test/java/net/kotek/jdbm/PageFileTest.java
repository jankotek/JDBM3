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

import java.io.File;

/**
 * This class contains all Unit tests for {@link PageFile}.
 */
final public class PageFileTest
        extends TestCaseWithTestFile {


    public static void deleteFile(String filename) {
        File file = new File(filename);

        if (file.exists()) {
            try {
                file.delete();
            } catch (Exception except) {
                except.printStackTrace();
            }
            if (file.exists()) {
                System.out.println("WARNING:  Cannot delete file: " + file);
            }
        }
    }


    /**
     * Test constructor
     */
    public void testCtor()
            throws Exception {
        PageFile file = newRecordFile();
        file.close();
    }


    /**
     * Test addition of record 0
     */
    public void testAddZero()
            throws Exception {
        String f = newTestFile();
        PageFile file = new PageFile(f);
        PageIo data = file.get(0);
        data.writeByte(14, (byte) 'b');
        file.release(0, true);
        file.close();
        file = new PageFile(f);
        data = file.get(0);
        assertEquals((byte) 'b', data.readByte(14));
        file.release(0, false);
        file.close();
    }


    /**
     * Test addition of a number of records, with holes.
     */
    public void testWithHoles()
            throws Exception {
        String f = newTestFile();
        PageFile file = new PageFile(f);

        // Write recid 0, byte 0 with 'b'
        PageIo data = file.get(0);
        data.writeByte(0,(byte) 'b');
        file.release(0, true);

        // Write recid 10, byte 10 with 'c'
        data = file.get(10);
        data.writeByte(10, (byte) 'c');
        file.release(10, true);

        // Write recid 5, byte 5 with 'e'
        data = file.get(5);
        data.writeByte(5, (byte) 'e');
        file.release(5, false);

        file.close();

        file = new PageFile(f);
        data = file.get(0);
        assertEquals("0 = b", (byte) 'b', data.readByte(0));
        file.release(0, false);

        data = file.get(5);
        assertEquals("5 = 0",  (byte) 'e', data.readByte(5));
        file.release(5, false);

        data = file.get(10);
        assertEquals("10 = c", (byte) 'c', data.readByte(10));
        file.release(10, false);

        file.close();
    }


    /**
     * Test wrong release
     */
    public void testWrongRelease()
            throws Exception {
        PageFile file = newRecordFile();

        // Write recid 0, byte 0 with 'b'
        PageIo data = file.get(0);
        data.writeByte(0,  (byte) 'b');
        try {
            file.release(1, true);
            fail("expected exception");
        } catch (NullPointerException except) {
            // ignore
        }
        file.release(0, false);

        file.close();

        // @alex retry to open the file
        /*
        file = new PageFile( testFileName );
        PageManager pm = new PageManager( file );
        pm.close();
        file.close();
        */
    }


}
