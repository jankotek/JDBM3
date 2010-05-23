/*
 *  $Id: TestFreeLogicalRowIdPage.java,v 1.3 2003/09/21 15:49:02 boisvert Exp $
 *
 *  Unit tests for FreeLogicalRowIdPage class
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

import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 *  This class contains all Unit tests for {@link FreeLogicalRowIdPage}.
 */
public class TestFreeLogicalRowIdPage extends TestCase {

    public TestFreeLogicalRowIdPage(String name) {
  super(name);
    }


    /**
     *  Test constructor - create a page
     */
    public void testCtor() throws Exception {
  byte[] data = new byte[RecordFile.BLOCK_SIZE];
  BlockIo test = new BlockIo(0, data);
  new PageHeader(test, Magic.FREELOGIDS_PAGE);
  FreeLogicalRowIdPage page = new FreeLogicalRowIdPage(test);
    }

    /**
     *  Test basics
     */
    public void testBasics() throws Exception {
  byte[] data = new byte[RecordFile.BLOCK_SIZE];
  BlockIo test = new BlockIo(0, data);
  new PageHeader(test, Magic.FREELOGIDS_PAGE);
  FreeLogicalRowIdPage page = new FreeLogicalRowIdPage(test);

  // we have a completely empty page.
  assertEquals("zero count", 0, page.getCount());

  // three allocs
  PhysicalRowId id = page.alloc(0);
  id = page.alloc(1);
  id = page.alloc(2);
  assertEquals("three count", 3, page.getCount());

  // setup last id (2)
  id.setBlock(1);
  id.setOffset((short) 2);

  // two frees
  page.free(0);
  page.free(1);
  assertEquals("one left count", 1, page.getCount());
  assertTrue("isfree 0", page.isFree(0));
  assertTrue("isfree 1", page.isFree(1));
  assertTrue("isalloc 2", page.isAllocated(2));

  // now, create a new page over the data and check whether
  // it's all the same.
  page = new FreeLogicalRowIdPage(test);

  assertEquals("2: one left count", 1, page.getCount());
  assertTrue("2: isfree 0", page.isFree(0));
  assertTrue("2: isfree 1", page.isFree(1));
  assertTrue("2: isalloc 2", page.isAllocated(2));

  id = page.get(2);
  assertEquals("block", 1, id.getBlock());
  assertEquals("offset", 2, id.getOffset());

    }


    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
  junit.textui.TestRunner.run(new TestSuite(TestFreeLogicalRowIdPage.class));
    }
}
