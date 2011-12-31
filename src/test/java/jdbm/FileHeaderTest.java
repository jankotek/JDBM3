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

import junit.framework.TestCase;

/**
 * This class contains all Unit tests for {@link FileHeader}.
 */
public class FileHeaderTest extends TestCase {

    public FileHeaderTest(String name) {
        super(name);
    }

    /**
     * Test set, write, read
     */
    public void testSetWriteRead() throws Exception {
        BlockIo b = new BlockIo(0, new byte[1000]);
        FileHeader f = new FileHeader(b, true);
        for (int i = 0; i < Magic.NLISTS; i++) {
            f.setFirstOf(i, 100 * i);
            f.setLastOf(i, 200 * i);
        }

        f = new FileHeader(b, false);
        for (int i = 0; i < Magic.NLISTS; i++) {
            assertEquals("first " + i, i * 100, f.getFirstOf(i));
            assertEquals("last " + i, i * 200, f.getLastOf(i));
        }
    }

    /**
     * Test root rowids
     */
    public void testRootRowids() throws Exception {
        BlockIo b = new BlockIo(0, new byte[Storage.BLOCK_SIZE]);
        FileHeader f = new FileHeader(b, true);
        for (int i = 0; i < FileHeader.NROOTS; i++) {
            f.setRoot(i, 100 * i);
        }

        f = new FileHeader(b, false);
        for (int i = 0; i < FileHeader.NROOTS; i++) {
            assertEquals("root " + i, i * 100, f.getRoot(i));
        }
    }

}
