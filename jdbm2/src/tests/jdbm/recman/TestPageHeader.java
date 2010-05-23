/*
 *  $Id: TestPageHeader.java,v 1.2 2003/09/21 15:49:02 boisvert Exp $
 *
 *  Unit tests for PageHeader class
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
