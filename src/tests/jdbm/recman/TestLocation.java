/*
 *  $Id: TestLocation.java,v 1.2 2003/09/21 15:49:02 boisvert Exp $
 *
 *  Unit tests for Location class
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
 * This class contains all Unit tests for {@link Location}.
 */
public class TestLocation extends TestCase {

	public TestLocation(String name) {
		super(name);
	}

	/**
	 * Basic tests
	 */
	public void testBasics() {

		Location loc = new Location(10, (short) 20);
		long longloc = loc.toLong();
		Location loc2 = new Location(longloc);
		assertEquals("longloc", longloc, loc2.toLong());

		assertEquals("block2", 10, loc2.getBlock());
		assertEquals("offset2", 20, loc2.getOffset());

	}

	/**
	 * Runs all tests in this class
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(new TestSuite(TestLocation.class));
	}
}
