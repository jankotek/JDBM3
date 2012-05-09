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
/*
 * Copyright (C) 2009 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jdbm;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * This code comes from GoogleCollections, was modified for JDBM by Jan Kotek
 *
 *
 *
 * Tests representing the contract of {@link SortedMap}. Concrete subclasses of
 * this base class test conformance of concrete {@link SortedMap} subclasses to
 * that contract.
 *
 * @author Jared Levy
 *
 *
 */
public class BTreeMapTest
        extends ConcurrentMapInterfaceTest<Integer, String> {

    public BTreeMapTest() {
        super(false, false, true, true, true, true);
    }

    DBAbstract r;

    public void setUp() throws Exception {
        r = TestCaseWithTestFile.newDBNoCache();
    }

    @Override
    protected Integer getKeyNotInPopulatedMap() throws UnsupportedOperationException {
        return -100;
    }

    @Override
    protected String getValueNotInPopulatedMap() throws UnsupportedOperationException {
        return "XYZ";
    }

    @Override
    protected String getSecondValueNotInPopulatedMap() throws UnsupportedOperationException {
        return "ASD";
    }

    @Override
    protected ConcurrentNavigableMap<Integer, String> makeEmptyMap() throws UnsupportedOperationException {
        try {
            BTree<Integer, String> b = BTree.createInstance(r);
            return new BTreeMap<Integer, String>(b, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected ConcurrentNavigableMap<Integer, String> makePopulatedMap() throws UnsupportedOperationException {
        ConcurrentNavigableMap<Integer, String> map = makeEmptyMap();
        for (int i = 0; i < 100; i++){
            if(i%11==0||i%7==0) continue;

            map.put(i, "aa" + i);
        }
        return map;
    }
    @Override
    protected ConcurrentNavigableMap<Integer, String> makeEitherMap() {
        try {
            return makePopulatedMap();
        } catch (UnsupportedOperationException e) {
            return makeEmptyMap();
        }
    }

    @SuppressWarnings("unchecked") // Needed for null comparator
    public void testOrdering() {
        final SortedMap<Integer, String> map;
        try {
            map = makePopulatedMap();
        } catch (UnsupportedOperationException e) {
            return;
        }
        Iterator<Integer> iterator = map.keySet().iterator();
        Integer prior = iterator.next();
        Comparator<? super Integer> comparator = map.comparator();
        while (iterator.hasNext()) {
            Integer current = iterator.next();
            if (comparator == null) {
                Comparable comparable = (Comparable) prior;
                assertTrue(comparable.compareTo(current) < 0);
            } else {
                assertTrue(map.comparator().compare(prior, current) < 0);
            }
            current = prior;
        }
    }

    public void testFirstKeyEmpty() {
        final SortedMap<Integer, String> map;
        try {
            map = makeEmptyMap();
        } catch (UnsupportedOperationException e) {
            return;
        }

            assertNull(map.firstKey());

        assertInvariants(map);
    }

    public void testFirstKeyNonEmpty() {
        final SortedMap<Integer, String> map;
        try {
            map = makePopulatedMap();
        } catch (UnsupportedOperationException e) {
            return;
        }
        Integer expected = map.keySet().iterator().next();
        assertEquals(expected, map.firstKey());
        assertInvariants(map);
    }

    public void testLastKeyEmpty() {
        final SortedMap<Integer, String> map;
        try {
            map = makeEmptyMap();
        } catch (UnsupportedOperationException e) {
            return;
        }
            assertNull(map.lastKey());
        assertInvariants(map);
    }

    public void testLastKeyNonEmpty() {
        final SortedMap<Integer, String> map;
        try {
            map = makePopulatedMap();
        } catch (UnsupportedOperationException e) {
            return;
        }
        Integer expected = null;
        for (Integer key : map.keySet()) {
            expected = key;
        }
        assertEquals(expected, map.lastKey());
        assertInvariants(map);
    }

    private static <E> List<E> toList(Collection<E> collection) {
        return new ArrayList<E>(collection);
    }

    private static <E> List<E> subListSnapshot(
            List<E> list, int fromIndex, int toIndex) {
        List<E> subList = new ArrayList<E>();
        for (int i = fromIndex; i < toIndex; i++) {
            subList.add(list.get(i));
        }
        return Collections.unmodifiableList(subList);
    }

    public void testHeadMap() {
        final NavigableMap<Integer, String> map;
        try {
            map = makeEitherMap();
        } catch (UnsupportedOperationException e) {
            return;
        }
        List<Entry<Integer, String>> list = toList(map.entrySet());
        for (int i = 0; i < list.size(); i++) {
            List<Entry<Integer, String>> expected = subListSnapshot(list, 0, i);
            SortedMap<Integer, String> headMap = map.headMap(list.get(i).getKey());
            assertEquals(expected, toList(headMap.entrySet()));
        }

        for (int i = 0; i < list.size(); i++) {
            List<Entry<Integer, String>> expected = subListSnapshot(list, 0, i+1);
            SortedMap<Integer, String> headMap = map.headMap(list.get(i).getKey(),true);
            assertEquals(expected, toList(headMap.entrySet()));
        }

        for (int i = 0; i < list.size(); i++) {
            List<Entry<Integer, String>> expected = subListSnapshot(list, 0, i);
            SortedMap<Integer, String> headMap = map.headMap(list.get(i).getKey(),false);
            assertEquals(expected, toList(headMap.entrySet()));
        }


    }



    public void testTailMap() {
        final NavigableMap<Integer, String> map;
        try {
            map = makeEitherMap();
        } catch (UnsupportedOperationException e) {
            return;
        }
        List<Entry<Integer, String>> list = toList(map.entrySet());
        for (int i = 0; i < list.size(); i++) {
            List<Entry<Integer, String>> expected = subListSnapshot(list, i, list.size());
            SortedMap<Integer, String> tailMap = map.tailMap(list.get(i).getKey());
            assertEquals(expected, toList(tailMap.entrySet()));
        }

        for (int i = 0; i < list.size(); i++) {
            List<Entry<Integer, String>> expected = subListSnapshot(list, i, list.size());
            SortedMap<Integer, String> tailMap = map.tailMap(list.get(i).getKey(),true);
            assertEquals(expected, toList(tailMap.entrySet()));
        }

        for (int i = 0; i < list.size(); i++) {
            List<Entry<Integer, String>> expected = subListSnapshot(list, i+1, list.size());
            SortedMap<Integer, String> tailMap = map.tailMap(list.get(i).getKey(),false);
            assertEquals(expected, toList(tailMap.entrySet()));
        }


    }


    public void testSubMap() {
        final NavigableMap<Integer, String> map;
        try {
            map = makeEitherMap();
        } catch (UnsupportedOperationException e) {
            return;
        }
        List<Entry<Integer, String>> list = toList(map.entrySet());
        for (int i = 0; i < list.size(); i++) {
            for (int j = i; j < list.size(); j++) {
                List<Entry<Integer, String>> expected = subListSnapshot(list, i, j);
                SortedMap<Integer, String> subMap
                        = map.subMap(list.get(i).getKey(), list.get(j).getKey());
                assertEquals(expected, toList(subMap.entrySet()));
                assertEquals(expected.size(), subMap.size());
                assertEquals(expected.size(), subMap.keySet().size());
                assertEquals(expected.size(), subMap.entrySet().size());
                assertEquals(expected.size(), subMap.values().size());
            }
        }

        for (int i = 0; i < list.size(); i++) {
            for (int j = i; j < list.size(); j++) {
                List<Entry<Integer, String>> expected = subListSnapshot(list, i, j+1);
                SortedMap<Integer, String> subMap
                        = map.subMap(list.get(i).getKey(), true, list.get(j).getKey(), true);
                assertEquals(expected, toList(subMap.entrySet()));
                assertEquals(expected.size(), subMap.size());
                assertEquals(expected.size(), subMap.keySet().size());
                assertEquals(expected.size(), subMap.entrySet().size());
                assertEquals(expected.size(), subMap.values().size());
            }
        }


        for (int i = 0; i < list.size(); i++) {
            for (int j = i; j < list.size(); j++) {
                List<Entry<Integer, String>> expected = subListSnapshot(list, i+1, j);
                SortedMap<Integer, String> subMap
                        = map.subMap(list.get(i).getKey(), false, list.get(j).getKey(), false);
                assertEquals(expected, toList(subMap.entrySet()));
                assertEquals(expected.size(), subMap.size());
                assertEquals(expected.size(), subMap.keySet().size());
                assertEquals(expected.size(), subMap.entrySet().size());
                assertEquals(expected.size(), subMap.values().size());
            }
        }



    }

    public void testSubMapIllegal() {
        final SortedMap<Integer, String> map;
        try {
            map = makePopulatedMap();
        } catch (UnsupportedOperationException e) {
            return;
        }
        if (map.size() < 2) {
            return;
        }
        Iterator<Integer> iterator = map.keySet().iterator();
        Integer first = iterator.next();
        Integer second = iterator.next();
        try {
            map.subMap(second, first);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
    }




}
