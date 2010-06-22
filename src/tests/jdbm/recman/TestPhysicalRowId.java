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
 *  This class contains all Unit tests for {@link PhysicalRowId}.
 */
public class TestPhysicalRowId extends TestCase {

    public TestPhysicalRowId(String name) {
  super(name);
    }
    

    /**
     *  Test basics - read and write at an offset
     */
    public void testReadWrite() throws Exception {
  byte[] data = new byte[RecordFile.BLOCK_SIZE];
  BlockIo test = new BlockIo(0, data);
  PhysicalRowId rowid = new PhysicalRowId(test, (short) 6);
  rowid.setBlock(1000);
  rowid.setOffset((short) 2345);
  
  assertEquals("block", 1000, rowid.getBlock());
  assertEquals("offset", 2345, rowid.getOffset());
    }
    
    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
  junit.textui.TestRunner.run(new TestSuite(TestPhysicalRowId.class));
    }
}
