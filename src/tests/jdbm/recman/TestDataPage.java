/*
 *  $Id: TestDataPage.java,v 1.2 2003/09/21 15:49:02 boisvert Exp $
 *
 *  Unit tests for DataPage class
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
 *  This class contains all Unit tests for {@link DataPage}.
 */
public class TestDataPage extends TestCase {

    public TestDataPage(String name) {
    	super(name);
    }
    

    /**
     *  Test basics - read and write at an offset
     */
    public void testReadWrite() throws Exception {
  byte[] data = new byte[RecordFile.BLOCK_SIZE];
  BlockIo test = new BlockIo(0, data);
  test.writeShort(0, (short) (Magic.BLOCK + Magic.USED_PAGE));
  
  DataPage page = new DataPage(test);
  page.setFirst((short) 1000);
  
  assertEquals("first", 1000, page.getFirst());
    }

    /**
     *  Test factory method.
     */
    public void testFactory() throws Exception {
  byte[] data = new byte[RecordFile.BLOCK_SIZE];
  BlockIo test = new BlockIo(0, data);
  test.writeShort(0, (short) (Magic.BLOCK + Magic.USED_PAGE));

  DataPage page = DataPage.getDataPageView(test);
  page.setFirst((short) 1000);
  
  assertEquals("first", 1000, page.getFirst());
    }
    
    
    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
  junit.textui.TestRunner.run(new TestSuite(TestDataPage.class));
    }
}
