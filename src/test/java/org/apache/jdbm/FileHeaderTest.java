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

package org.apache.jdbm;

import junit.framework.TestCase;

public class FileHeaderTest extends TestCase {


    /**
     * Test set, write, read
     */
    public void testSetWriteRead() throws Exception {
        PageIo b = new PageIo(0, new byte[1000]);
        b.fileHeaderCheckHead(true);        
        for (int i = 0; i < Magic.NLISTS; i++) {
            b.fileHeaderSetFirstOf(i, 100 * i);
            b.fileHeaderSetLastOf(i, 200 * i);
        }

        b.fileHeaderCheckHead(false);
        for (int i = 0; i < Magic.NLISTS; i++) {
            assertEquals("first " + i, i * 100, b.fileHeaderGetFirstOf(i));
            assertEquals("last " + i, i * 200, b.fileHeaderGetLastOf(i));
        }
    }

    /**
     * Test root rowids
     */
    public void testRootRowids() throws Exception {
        PageIo b = new PageIo(0, new byte[Storage.PAGE_SIZE]);
        b.fileHeaderCheckHead(true);
        for (int i = 0; i < Magic.FILE_HEADER_NROOTS; i++) {
            b.fileHeaderSetRoot(i, 100 * i);
        }

        b.fileHeaderCheckHead(false);
        for (int i = 0; i < Magic.FILE_HEADER_NROOTS; i++) {
            assertEquals("root " + i, i * 100, b.fileHeaderGetRoot(i));
        }
    }

}
