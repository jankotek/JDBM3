/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.kotek.jdbm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * Tests for LinkedList2 which comes with JDBM. Original code comes from Apache Harmony,
 * Modified by Jan Kotek for use in JDBM
 */
public class LinkedList2Test extends TestCaseWithTestFile {

    DB db;

    LinkedList2 ll;

    LinkedList2<Object> testList;

    private Object testObjOne;

    private Object testObjTwo;

    private Object testObjThree;

    private Object testObjFour;

    private Object testObjLast;

    static Object[] objArray;

    {
        objArray = new Object[100];
        for (int i = 0; i < objArray.length; i++)
            objArray[i] = new Integer(i);
    }


    /**
     * @tests java.util.LinkedList2#add(int, java.lang.Object)
     */
    public void test_addILjava_lang_Object() {
        // Test for method void java.util.LinkedList2.add(int, java.lang.Object)
        Object o = "Test";
        ll.add(50, o);
        assertEquals("Failed to add Object>: " + ll.get(50).toString(), ll
                .get(50), o);
        assertEquals("Failed to fix up list after insert", ll.get(51), objArray[50]);
        assertEquals(ll.get(52), objArray[51]);
        ll.add(50, null);
        assertNull("Did not add null correctly", ll.get(50));

        try {
            ll.add(-1, "Test");
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Excepted
        }

        try {
            ll.add(-1, null);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Excepted
        }

        try {
            ll.add(ll.size() + 1, "Test");
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Excepted
        }

        try {
            ll.add(ll.size() + 1, null);
            fail("Should throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException e) {
            // Excepted
        }
    }


    /**
     * @tests java.util.LinkedList2#addAll(int, java.util.Collection)
     */
    public void test_addAllILjava_util_Collection() {
        // Test for method boolean java.util.LinkedList2.addAll(int,
        // java.util.Collection)
        ll.addAll(50, new ArrayList(ll));
        assertEquals("Returned incorrect size after adding to existing list", 200, ll
                .size());
        for (int i = 0; i < 50; i++)
            assertEquals("Manipulated elements < index", ll.get(i), objArray[i]);
        for (int i = 0; i >= 50 && (i < 150); i++)
            assertTrue("Failed to ad elements properly",
                    ll.get(i) == objArray[i - 50]);
        for (int i = 0; i >= 150 && (i < 200); i++)
            assertTrue("Failed to ad elements properly",
                    ll.get(i) == objArray[i - 100]);
        List myList = db.createLinkedList("testXX");
        myList.add(null);
        myList.add("Blah");
        myList.add(null);
        myList.add("Booga");
        myList.add(null);
        ll.addAll(50, myList);
        assertNull("a) List w/nulls not added correctly", ll.get(50));
        assertEquals("b) List w/nulls not added correctly",
                "Blah", ll.get(51));
        assertNull("c) List w/nulls not added correctly", ll.get(52));
        assertEquals("d) List w/nulls not added correctly",
                "Booga", ll.get(53));
        assertNull("e) List w/nulls not added correctly", ll.get(54));

        try {
            ll.addAll(50, null);
            fail("Should throw NullPointerException");
        } catch (NullPointerException e) {
            // Excepted
        }
    }

    /**
     * @tests java.util.LinkedList2#addAll(int, java.util.Collection)
     */
    public void test_addAllILjava_util_Collection_2() {
        // Regression for HARMONY-467
        LinkedList2 obj = (LinkedList2) db.createLinkedList("testXX");
        try {
            obj.addAll(-1, (Collection) null);
            fail("IndexOutOfBoundsException expected");
        } catch (IndexOutOfBoundsException e) {
        }
    }

    /**
     * @tests java.util.LinkedList2#addAll(java.util.Collection)
     */
    public void test_addAllLjava_util_Collection() {

        // Test for method boolean
        // java.util.LinkedList2.addAll(java.util.Collection)
        List l = new ArrayList();
        l.addAll(new ArrayList(ll));

        for (int i = 0; i < ll.size(); i++)
            assertTrue("Failed to add elements properly", l.get(i).equals(
                    ll.get(i)));
        ll.addAll(new ArrayList(ll));
        assertEquals("Returned incorrect siZe after adding to existing list", 200, ll
                .size());
        for (int i = 0; i < 100; i++) {
            assertTrue("Added to list in incorrect order", ll.get(i).equals(
                    l.get(i)));
            assertTrue("Failed to add to existing list", ll.get(i + 100)
                    .equals(l.get(i)));
        }
        List myList = db.createLinkedList("testXX");
        myList.add(null);
        myList.add("Blah");
        myList.add(null);
        myList.add("Booga");
        myList.add(null);
        ll.addAll(myList);
        assertNull("a) List w/nulls not added correctly", ll.get(200));
        assertEquals("b) List w/nulls not added correctly",
                "Blah", ll.get(201));
        assertNull("c) List w/nulls not added correctly", ll.get(202));
        assertEquals("d) List w/nulls not added correctly",
                "Booga", ll.get(203));
        assertNull("e) List w/nulls not added correctly", ll.get(204));

        try {
            ll.addAll(null);
            fail("Should throw NullPointerException");
        } catch (NullPointerException e) {
            // Excepted
        }
    }


    /**
     * @tests java.util.LinkedList2#clear()
     */
    public void test_clear() {
        // Test for method void java.util.LinkedList2.clear()
        ll.clear();
        for (int i = 0; i < ll.size(); i++)
            assertNull("Failed to clear list", ll.get(i));
    }


    /**
     * @tests java.util.LinkedList2#contains(java.lang.Object)
     */
    public void test_containsLjava_lang_Object() {
        // Test for method boolean
        // java.util.LinkedList2.contains(java.lang.Object)
        assertTrue("Returned false for valid element", ll
                .contains(objArray[99]));
        assertTrue("Returned false for equal element", ll.contains(new Integer(
                8)));
        assertTrue("Returned true for invalid element", !ll
                .contains(new Object()));
        assertTrue("Should not contain null", !ll.contains(null));
        ll.add(25, null);
        assertTrue("Should contain null", ll.contains(null));
    }

    /**
     * @tests java.util.LinkedList2#get(int)
     */
    public void test_getI() {
        // Test for method java.lang.Object java.util.LinkedList2.get(int)
        assertEquals("Returned incorrect element", ll.get(22), objArray[22]);
        try {
            ll.get(8765);
            fail("Failed to throw expected exception for index > size");
        } catch (IndexOutOfBoundsException e) {
        }
    }


    /**
     * @tests java.util.LinkedList2#indexOf(java.lang.Object)
     */
    public void test_indexOfLjava_lang_Object() {
        // Test for method int java.util.LinkedList2.indexOf(java.lang.Object)
        assertEquals("Returned incorrect index", 87, ll.indexOf(objArray[87]));
        assertEquals("Returned index for invalid Object", -1, ll
                .indexOf(new Object()));
        ll.add(20, null);
        ll.add(24, null);
        assertTrue("Index of null should be 20, but got: " + ll.indexOf(null),
                ll.indexOf(null) == 20);
    }

    /**
     * @tests java.util.LinkedList2#lastIndexOf(java.lang.Object)
     */
    public void test_lastIndexOfLjava_lang_Object() {
        // Test for method int
        // java.util.LinkedList2.lastIndexOf(java.lang.Object)
        ll.add(new Integer(99));
        assertEquals("Returned incorrect index",
                100, ll.lastIndexOf(objArray[99]));
        assertEquals("Returned index for invalid Object", -1, ll
                .lastIndexOf(new Object()));
        ll.add(20, null);
        ll.add(24, null);
        assertTrue("Last index of null should be 20, but got: "
                + ll.lastIndexOf(null), ll.lastIndexOf(null) == 24);
    }

    /**
     * @tests java.util.LinkedList2#listIterator(int)
     */
    public void test_listIteratorI() {
        // Test for method java.util.ListIterator
        // java.util.LinkedList2.listIterator(int)
        ListIterator i = ll.listIterator();
        Object elm;
        int n = 0;
        while (i.hasNext()) {
            if (n == 0 || n == objArray.length - 1) {
                if (n == 0)
                    assertTrue("First element claimed to have a previous", !i
                            .hasPrevious());
                if (n == objArray.length)
                    assertTrue("Last element claimed to have next", !i
                            .hasNext());
            }
            elm = i.next();
            assertEquals("Iterator returned elements in wrong order",
                    elm, objArray[n]);
            if (n > 0 && n < objArray.length - 1) {
                assertEquals("Next index returned incorrect value",
                        i.nextIndex(), n + 1);
                assertEquals("previousIndex returned incorrect value : "
                        + i.previousIndex() + ", n val: " + n, i
                        .previousIndex(), n);
            }
            ++n;
        }
        List myList = db.createLinkedList("testXX");
        myList.add(null);
        myList.add("Blah");
        myList.add(null);
        myList.add("Booga");
        myList.add(null);
        ListIterator li = myList.listIterator();
        assertTrue("li.hasPrevious() should be false", !li.hasPrevious());
        assertNull("li.next() should be null", li.next());
        assertTrue("li.hasPrevious() should be true", li.hasPrevious());
        assertNull("li.prev() should be null", li.previous());
        assertNull("li.next() should be null", li.next());
        assertEquals("li.next() should be Blah", "Blah", li.next());
        assertNull("li.next() should be null", li.next());
        assertEquals("li.next() should be Booga", "Booga", li.next());
        assertTrue("li.hasNext() should be true", li.hasNext());
        assertNull("li.next() should be null", li.next());
        assertTrue("li.hasNext() should be false", !li.hasNext());
    }

    /**
     * @tests java.util.LinkedList2#remove(int)
     */
    public void test_removeI() {
        // Test for method java.lang.Object java.util.LinkedList2.remove(int)
        ll.remove(10);
        assertEquals("Failed to remove element", -1, ll.indexOf(objArray[10]));
        try {
            ll.remove(999);
            fail("Failed to throw expected exception when index out of range");
        } catch (IndexOutOfBoundsException e) {
            // Correct
        }

        ll.add(20, null);
        ll.remove(20);
        assertNotNull("Should have removed null", ll.get(20));
    }

    /**
     * @tests java.util.LinkedList2#remove(java.lang.Object)
     */
    public void test_removeLjava_lang_Object() {
        // Test for method boolean java.util.LinkedList2.remove(java.lang.Object)
        assertTrue("Failed to remove valid Object", ll.remove(objArray[87]));
        assertTrue("Removed invalid object", !ll.remove(new Object()));
        assertEquals("Found Object after removal", -1, ll.indexOf(objArray[87]));
        ll.add(null);
        ll.remove(null);
        assertTrue("Should not contain null afrer removal", !ll.contains(null));
    }


    /**
     * @tests java.util.LinkedList2#set(int, java.lang.Object)
     */
    public void test_setILjava_lang_Object() {
        // Test for method java.lang.Object java.util.LinkedList2.set(int,
        // java.lang.Object)
        Object obj;
        ll.set(65, obj = new Object());
        assertEquals("Failed to set object", ll.get(65), obj);
    }

    /**
     * @tests java.util.LinkedList2#size()
     */
    public void test_size() {
        // Test for method int java.util.LinkedList2.size()
        assertEquals("Returned incorrect size", ll.size(), objArray.length);

        int counter = 0;
        Iterator iter = ll.iterator();
        while (iter.hasNext()) {
            counter++;
            iter.next();
        }
        assertEquals("Returned incorrect size", counter, objArray.length);

        ll.remove(0);
        assertEquals("Returned incorrect size", ll.size(), objArray.length - 1);
    }

    /**
     * @tests java.util.LinkedList2#toArray()
     */
    public void test_toArray() {
        // Test for method java.lang.Object [] java.util.LinkedList2.toArray()
        ll.add(null);
        Object[] obj = ll.toArray();
        assertEquals("Returned array of incorrect size", objArray.length + 1, obj.length);

        for (int i = 0; i < obj.length - 1; i++)
            assertEquals("Returned incorrect array: " + i, obj[i], objArray[i]);
        assertNull("Returned incorrect array--end isn't null",
                obj[obj.length - 1]);
    }

    /**
     * @tests java.util.LinkedList2#toArray(java.lang.Object[])
     */
    public void test_toArray$Ljava_lang_Object() {
        // Test for method java.lang.Object []
        // java.util.LinkedList2.toArray(java.lang.Object [])
        Integer[] argArray = new Integer[100];
        Object[] retArray;
        retArray = ll.toArray(argArray);
        assertTrue("Returned different array than passed", retArray == argArray);
        List retList = db.createLinkedList("testXX1");
        retList.addAll(Arrays.asList(retArray));
        Iterator li = ll.iterator();
        Iterator ri = retList.iterator();
        while (li.hasNext())
            assertTrue("Lists are not equal", li.next() == ri.next());
        argArray = new Integer[1000];
        retArray = ll.toArray(argArray);
        assertNull("Failed to set first extra element to null", argArray[ll
                .size()]);
        for (int i = 0; i < ll.size(); i++)
            assertEquals("Returned incorrect array: " + i,
                    retArray[i], objArray[i]);
        ll.add(50, null);
        argArray = new Integer[101];
        retArray = ll.toArray(argArray);
        assertTrue("Returned different array than passed", retArray == argArray);
        retArray = ll.toArray(argArray);
        assertTrue("Returned different array than passed", retArray == argArray);
        retList = db.createLinkedList("testXX2");
        retList.addAll(Arrays.asList(retArray));
        li = ll.iterator();
        ri = retList.iterator();
        while (li.hasNext())
            assertTrue("Lists are not equal", li.next() == ri.next());
    }


    /**
     * @tests {@link java.util.LinkedList#remove()}
     */
    public void test_remove() {
        for (int i = 0; i < objArray.length; i++) {
            assertEquals("should remove the head", objArray[i], ll.remove(0));
        }
        assertEquals("should be empty", 0, ll.size());
        try {
            ll.remove(0);
            fail("IndexOutOfBoundsException is expected when removing from the empty list");
        } catch (IndexOutOfBoundsException e) {
            //-- expected
        }
    }


    /**
     * Sets up the fixture, for example, open a network connection. This method
     * is called before a test is executed.
     */
    public void setUp() throws Exception {
        super.setUp();
        this.db = newDBCache();
        ll = (LinkedList2) db.createLinkedList("ll");
        for (int i = 0; i < objArray.length; i++) {
            ll.add(objArray[i]);
        }
        testList = (LinkedList2<Object>) db.createLinkedList("testList");
        testObjOne = new Object();
        testObjTwo = new Object();
        testObjThree = new Object();
        testObjFour = new Object();
        testObjLast = new Object();
    }
}