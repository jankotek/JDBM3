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

package org.apache.jdbm;

import java.util.*;

/**
 * Wrapper class for <code>>SortedMap</code> to implement <code>>NavigableSet</code>
 * <p/>
 * This code originally comes from Apache Harmony, was adapted by Jan Kotek for JDBM
 */
class BTreeSet<E> extends AbstractSet<E> implements NavigableSet<E> {


    /**
     * use keyset from this map
     */
    final BTreeMap<E, Object> map;

    BTreeSet(BTreeMap<E, Object> map) {
        this.map = map;
    }

    public boolean add(E object) {
        return map.put(object, Utils.EMPTY_STRING) == null;
    }


    public boolean addAll(Collection<? extends E> collection) {
        return super.addAll(collection);
    }


    public void clear() {
        map.clear();
    }

    public Comparator<? super E> comparator() {
        return map.comparator();
    }


    public boolean contains(Object object) {
        return map.containsKey(object);
    }


    public boolean isEmpty() {
        return map.isEmpty();
    }


    public E lower(E e) {
        return map.lowerKey(e);
    }

    public E floor(E e) {
        return map.floorKey(e);
    }

    public E ceiling(E e) {
        return map.ceilingKey(e);
    }

    public E higher(E e) {
        return map.higherKey(e);
    }

    public E pollFirst() {
        Map.Entry<E,Object> e = map.pollFirstEntry();
        return e!=null? e.getKey():null;
    }

    public E pollLast() {
        Map.Entry<E,Object> e = map.pollLastEntry();
        return e!=null? e.getKey():null;
    }

    public Iterator<E> iterator() {
        final Iterator<Map.Entry<E,Object>> iter = map.entrySet().iterator();
        return new Iterator<E>() {
            public boolean hasNext() {
                return iter.hasNext();
            }

            public E next() {
                Map.Entry<E,Object> e = iter.next();
                return e!=null?e.getKey():null;
            }

            public void remove() {
                iter.remove();
            }
        };
    }

    public NavigableSet<E> descendingSet() {
        return map.descendingKeySet();
    }

    public Iterator<E> descendingIterator() {
        return map.descendingKeySet().iterator();
    }

    public NavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
        return map.subMap(fromElement,fromInclusive,toElement,toInclusive).navigableKeySet();
    }

    public NavigableSet<E> headSet(E toElement, boolean inclusive) {
        return map.headMap(toElement,inclusive).navigableKeySet();
    }

    public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
        return map.tailMap(fromElement,inclusive).navigableKeySet();
    }


    public boolean remove(Object object) {
        return map.remove(object) != null;
    }

    public int size() {
        return map.size();
    }


    public E first() {
        return map.firstKey();
    }


    public E last() {
        return map.lastKey();
    }


    public SortedSet<E> subSet(E start, E end) {
        Comparator<? super E> c = map.comparator();
        int compare = (c == null) ? ((Comparable<E>) start).compareTo(end) : c
                .compare(start, end);
        if (compare <= 0) {
            return new BTreeSet<E>((BTreeMap<E,Object>) map.subMap(start, true,end,false));
        }
        throw new IllegalArgumentException();
    }


    public SortedSet<E> headSet(E end) {
        // Check for errors
        Comparator<? super E> c = map.comparator();
        if (c == null) {
            ((Comparable<E>) end).compareTo(end);
        } else {
            c.compare(end, end);
        }
        return new BTreeSet<E>((BTreeMap<E,Object>) map.headMap(end,false));
    }


    public SortedSet<E> tailSet(E start) {
        // Check for errors
        Comparator<? super E> c = map.comparator();
        if (c == null) {
            ((Comparable<E>) start).compareTo(start);
        } else {
            c.compare(start, start);
        }
        return new BTreeSet<E>((BTreeMap<E,Object>) map.tailMap(start,true));
    }


}
