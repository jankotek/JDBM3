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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import junit.framework.TestCase;

/**
 * This class contains all Unit tests for {@link BlockIo}.
 */
public class BlockIoTest extends TestCase {

    private static final short SHORT_VALUE = 0x1234;
    private static final int INT_VALUE = 0xe7b3c8a1;
    private static final long LONG_VALUE = 0xfdebca9876543210L;
    private static final long LONG_VALUE2 = 1231290495545446485L;


    /**
     * Test writing
     */
    public void testWrite() throws Exception {
        byte[] data = new byte[100];
        BlockIo test = new BlockIo(0, data);
        test.writeShort(0, SHORT_VALUE);
        test.writeLong(2, LONG_VALUE);
        test.writeInt(10, INT_VALUE);
        test.writeLong(14, LONG_VALUE2);

        DataInputStream is =
                new DataInputStream(new ByteArrayInputStream(data));
        assertEquals("short", SHORT_VALUE, is.readShort());
        assertEquals("long", LONG_VALUE, is.readLong());
        assertEquals("int", INT_VALUE, is.readInt());
        assertEquals("long", LONG_VALUE2, is.readLong());

        assertEquals("short", SHORT_VALUE, test.readShort(0));
        assertEquals("long", LONG_VALUE, test.readLong(2));
        assertEquals("int", INT_VALUE, test.readInt(10));
        assertEquals("long", LONG_VALUE2, test.readLong(14));

    }

    /**
     * Test reading
     */
    public void testRead() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(100);
        DataOutputStream os = new DataOutputStream(bos);
        os.writeShort(SHORT_VALUE);
        os.writeLong(LONG_VALUE);
        os.writeInt(INT_VALUE);
        os.writeLong(LONG_VALUE2);

        byte[] data = bos.toByteArray();
        BlockIo test = new BlockIo(0, data);
        assertEquals("short", SHORT_VALUE, test.readShort(0));
        assertEquals("long", LONG_VALUE, test.readLong(2));
        assertEquals("int", INT_VALUE, test.readInt(10));
        assertEquals("long", LONG_VALUE2, test.readLong(14));
    }


}
