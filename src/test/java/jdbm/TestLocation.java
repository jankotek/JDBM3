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

		long loc = Location.toLong(10, (short) 20);

		assertEquals("block2", 10, Location.getBlock(loc));
		assertEquals("offset2", 20, Location.getOffset(loc));

	}

}
