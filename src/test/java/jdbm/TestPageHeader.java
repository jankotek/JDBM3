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
import junit.framework.TestSuite;

/**
 *  This class contains all Unit tests for {@link PageHeader}.
 */
public class TestPageHeader extends TestCase {

    public TestPageHeader(String name) {
  super(name);
    }

    /**
     *  Test set, write, read
     */
    public void testSetWriteRead() throws Exception {
  BlockIo data = new BlockIo(0, new byte[RecordFile.BLOCK_SIZE]);
  PageHeader p = new PageHeader(data, Magic.FREE_PAGE);
  p.setNext(10);
  p.setPrev(33);
  
  p = new PageHeader(data);
  assertEquals("next", 10, p.getNext());
  assertEquals("prev", 33, p.getPrev());
    }

    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
  junit.textui.TestRunner.run(new TestSuite(TestPageHeader.class));
    }
}
