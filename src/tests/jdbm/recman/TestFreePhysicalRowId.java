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

import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 *  This class contains all Unit tests for {@link FreePhysicalRowId}.
 */
public class TestFreePhysicalRowId extends TestCase {

    private static final short SHORT_VALUE = 0x1234;
    private static final long LONG_VALUE = 0xfdebca9876543210L;

    public TestFreePhysicalRowId(String name) {
  super(name);
    }
    

    /**
     *  Test basics - read and write at an offset
     */
    public void testReadWrite() throws Exception {
  byte[] data = new byte[100];
  BlockIo test = new BlockIo(0, data);
  FreePhysicalRowId rowid = new FreePhysicalRowId(test, (short) 6);
  rowid.setBlock(1000);
  rowid.setOffset((short) 2345);
  rowid.setSize(0xabcdef);
  
  assertEquals("block", 1000, rowid.getBlock());
  assertEquals("offset", 2345, rowid.getOffset());
  assertEquals("size", 0xabcdef, rowid.getSize());
    }
    
    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
  junit.textui.TestRunner.run(new TestSuite(TestFreePhysicalRowId.class));
    }
}
