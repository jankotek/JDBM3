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
 *  This class contains all Unit tests for {@link FreeLogicalRowIdPage}.
 */
public class FreeLogicalRowIdPageTest extends TestCase {

    public FreeLogicalRowIdPageTest(String name) {
  super(name);
    }


    /**
     *  Test constructor - create a page
     */
    public void testCtor() throws Exception {
  byte[] data = new byte[Storage.BLOCK_SIZE];
  BlockIo test = new BlockIo(0, data);
  new PageHeader(test, Magic.FREELOGIDS_PAGE);
  FreeLogicalRowIdPage page = new FreeLogicalRowIdPage(test, Storage.BLOCK_SIZE);
    }

    /**
     *  Test basics
     */
    public void testBasics() throws Exception {
  byte[] data = new byte[Storage.BLOCK_SIZE];
  BlockIo test = new BlockIo(0, data);
  new PageHeader(test, Magic.FREELOGIDS_PAGE);
  FreeLogicalRowIdPage page = new FreeLogicalRowIdPage(test, Storage.BLOCK_SIZE);

  // we have a completely empty page.
  assertEquals("zero count", 0, page.getCount());

  // three allocs
  short id = page.alloc(0);
  id = page.alloc(1);
  id = page.alloc(2);
  assertEquals("three count", 3, page.getCount());

  // setup last id (2)
  page.setLocationBlock(id, 1);
  page.setLocationOffset(id,(short) 2);

  // two frees
  page.free(0);
  page.free(1);
  assertEquals("one left count", 1, page.getCount());
  assertTrue("isfree 0", page.isFree(0));
  assertTrue("isfree 1", page.isFree(1));
  assertTrue("isalloc 2", page.isAllocated(2));

  // now, create a new page over the data and check whether
  // it's all the same.
  page = new FreeLogicalRowIdPage(test, Storage.BLOCK_SIZE);

  assertEquals("2: one left count", 1, page.getCount());
  assertTrue("2: isfree 0", page.isFree(0));
  assertTrue("2: isfree 1", page.isFree(1));
  assertTrue("2: isalloc 2", page.isAllocated(2));

  id = page.slotToOffset(2);
  assertEquals("block", 1, page.getLocationBlock(id));
  assertEquals("offset", 2, page.getLocationOffset(id));

    }


}
