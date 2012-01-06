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

import java.util.*;

/**
 * TreeSet is an implementation of SortedSet. All optional operations (adding
 * and removing) are supported. The elements can be any objects which are
 * comparable to each other either using their natural order or a specified
 * Comparator.
 * <p/>
 * This code originally comes from Apache Harmony, was adapted by Jan Kotek for JDBM
 */
class BTreeSet<E> extends AbstractSet<E> implements SortedSet<E> {


    /**
     * Keys are this set's elements. Values are always Boolean.TRUE
     */
    private SortedMap<E, Object> backingMap;

    BTreeSet(SortedMap<E, Object> map) {
        backingMap = map;
    }

    public boolean add(E object) {
        return backingMap.put(object, Utils.EMPTY_STRING) == null;
    }


    public boolean addAll(Collection<? extends E> collection) {
        return super.addAll(collection);
    }


    public void clear() {
        backingMap.clear();
    }

    public Comparator<? super E> comparator() {
        return backingMap.comparator();
    }


    public boolean contains(Object object) {
        return backingMap.containsKey(object);
    }


    public boolean isEmpty() {
        return backingMap.isEmpty();
    }


    public Iterator<E> iterator() {
        return backingMap.keySet().iterator();
    }


    public boolean remove(Object object) {
        return backingMap.remove(object) != null;
    }

    public int size() {
        return backingMap.size();
    }


    public E first() {
        return backingMap.firstKey();
    }


    public E last() {
        return backingMap.lastKey();
    }


    public SortedSet<E> subSet(E start, E end) {
        Comparator<? super E> c = backingMap.comparator();
        int compare = (c == null) ? ((Comparable<E>) start).compareTo(end) : c
                .compare(start, end);
        if (compare <= 0) {
            return new BTreeSet<E>(backingMap.subMap(start, end));
        }
        throw new IllegalArgumentException();
    }


    public SortedSet<E> headSet(E end) {
        // Check for errors
        Comparator<? super E> c = backingMap.comparator();
        if (c == null) {
            ((Comparable<E>) end).compareTo(end);
        } else {
            c.compare(end, end);
        }
        return new BTreeSet<E>(backingMap.headMap(end));
    }


    public SortedSet<E> tailSet(E start) {
        // Check for errors
        Comparator<? super E> c = backingMap.comparator();
        if (c == null) {
            ((Comparable<E>) start).compareTo(start);
        } else {
            c.compare(start, start);
        }
        return new BTreeSet<E>(backingMap.tailMap(start));
    }


}