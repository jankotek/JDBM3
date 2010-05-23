/*
 *  $Id: TestBlockIo.java,v 1.1 2000/05/06 00:00:53 boisvert Exp $
 *
 *  Unit tests for BlockIo class
 *
 *  Simple db toolkit
 *  Copyright (C) 1999, 2000 Cees de Groot <cg@cdegroot.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Library General Public License 
 *  as published by the Free Software Foundation; either version 2 
 *  of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Library General Public License for more details.
 *
 *  You should have received a copy of the GNU Library General Public License 
 *  along with this library; if not, write to the Free Software Foundation, 
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 */
package jdbm.recman;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 *  This class contains all Unit tests for {@link BlockIo}.
 */
public class TestBlockIo extends TestCase {

    private static final short SHORT_VALUE = 0x1234;
    private static final int INT_VALUE = 0xe7b3c8a1;
    private static final long LONG_VALUE = 0xfdebca9876543210L;

    public TestBlockIo(String name) {
  super(name);
    }
    

    /**
     *  Test writing
     */
    public void testWrite() throws Exception {
  byte[] data = new byte[100];
  BlockIo test = new BlockIo(0, data);
  test.writeShort(0, SHORT_VALUE);
  test.writeLong(2, LONG_VALUE);
  test.writeInt(10, INT_VALUE);
  
  DataInputStream is = 
      new DataInputStream(new ByteArrayInputStream(data));
  assertEquals("short", SHORT_VALUE, is.readShort());
  assertEquals("long", LONG_VALUE, is.readLong());
  assertEquals("int", INT_VALUE, is.readInt());
    }

    /**
     *  Test reading
     */
    public void testRead() throws Exception {
  ByteArrayOutputStream bos = new ByteArrayOutputStream(100);
  DataOutputStream os = new DataOutputStream(bos);
  os.writeShort(SHORT_VALUE);
  os.writeLong(LONG_VALUE);
  os.writeInt(INT_VALUE);

  byte[] data = bos.toByteArray();
  BlockIo test = new BlockIo(0, data);
  assertEquals("short", SHORT_VALUE, test.readShort(0));
  assertEquals("long", LONG_VALUE, test.readLong(2));
  assertEquals("int", INT_VALUE, test.readInt(10));
    }
    
    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
  junit.textui.TestRunner.run(new TestSuite(TestBlockIo.class));
    }
}
