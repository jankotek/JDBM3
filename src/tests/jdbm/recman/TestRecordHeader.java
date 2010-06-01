/*
 *  $Id: TestRecordHeader.java,v 1.2 2003/09/21 15:49:02 boisvert Exp $
 *
 *  Unit tests for RecordHeader class
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
 *  This class contains all Unit tests for {@link RecordHeader}.
 */
public class TestRecordHeader extends TestCase {

    public TestRecordHeader(String name) {
  super(name);
    }

    /**
     *  Test basics - read and write at an offset
     */
    public void testReadWrite() throws Exception {
  byte[] data = new byte[RecordFile.BLOCK_SIZE];
  BlockIo test = new BlockIo(0, data);
  RecordHeader hdr = new RecordHeader(test, (short) 6);
  hdr.setCurrentSize(1000);
  hdr.setAvailableSize((short) 2345);
  
  assertEquals("current size", 1000, hdr.getCurrentSize());
  assertEquals("available size", 2345, hdr.getAvailableSize());
    }
    
    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
  junit.textui.TestRunner.run(new TestSuite(TestRecordHeader.class));
    }
}
