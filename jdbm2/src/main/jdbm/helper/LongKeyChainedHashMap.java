/*
 *  Primitive Collections for Java.
 *  Copyright (C) 2003  S�ren Bak
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package jdbm.helper;

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *  This class represents chained hash table based maps from
 *  long values to objects.
 *
 *  @see        LongKeyOpenHashMap
 *  @see        java.util.Map
 *
 *  @author     S&oslash;ren Bak
 *  @version    1.2     21-08-2003 19:42
 *  @since      1.0
 *  
 *  ********* JDBM Project Note *************
 *  This class was extracted from the pcj project (with permission)
 *  for use in jdbm only.  Modifications to original were performed
 *  by Kevin Day to make it work outside of the pcj class structure.
 *  
 *  Striped even more by Jan Kotek as part of JDBM 2.0
 *  
 *  
 */
final public class LongKeyChainedHashMap<E> implements Serializable {

	private static final long serialVersionUID = 5732061193461471028L;

	/** Constant indicating relative growth policy. */
    private static final int    GROWTH_POLICY_RELATIVE      = 0;


    /**
     *  The default growth policy of this map.
     *  @see    #GROWTH_POLICY_RELATIVE
     *  @see    #GROWTH_POLICY_ABSOLUTE
     */
    private static final int    DEFAULT_GROWTH_POLICY       = GROWTH_POLICY_RELATIVE;

    /** The default factor with which to increase the capacity of this map. */
    public static final double DEFAULT_GROWTH_FACTOR        = 1.0;

    /** The default chunk size with which to increase the capacity of this map. */
    public static final int    DEFAULT_GROWTH_CHUNK         = 10;

    /** The default capacity of this map. */
    public static final int    DEFAULT_CAPACITY             = 11;

    /** The default load factor of this map. */
    public static final double DEFAULT_LOAD_FACTOR          = 0.75;


    /**
     *  The size of this map.
     *  @serial
     */
    private int size;

    /** The hash table backing up this map. Contains linked Entry<E> values. */
    private transient Entry<E>[] data;

    /**
     *  The growth policy of this map (0 is relative growth, 1 is absolute growth).
     *  @serial
     */
    private int growthPolicy;

    /**
     *  The growth factor of this map, if the growth policy is
     *  relative.
     *  @serial
     */
    private double growthFactor;

    /**
     *  The growth chunk size of this map, if the growth policy is
     *  absolute.
     *  @serial
     */
    private int growthChunk;

    /**
     *  The load factor of this map.
     *  @serial
     */
    private double loadFactor;

    /**
     *  The next size at which to expand the data[].
     *  @serial
     */
    private int expandAt;

//    /** A set view of the keys of this map. */
//    private transient LongSet keys;

    /** A collection view of the values of this map. */
    private transient Collection<E> values;

	private LongKeyChainedHashMap(int capacity, int growthPolicy, double growthFactor, int growthChunk, double loadFactor) {

        data = makeEntryArray(capacity);
        size = 0;
        expandAt = (int)Math.round(loadFactor*capacity);
        this.growthPolicy = growthPolicy;
        this.growthFactor = growthFactor;
        this.growthChunk = growthChunk;
        this.loadFactor = loadFactor;
    }

    /**
     * creates new map with given capacity
     */
    public LongKeyChainedHashMap(int capacity) {
    	this(capacity,DEFAULT_GROWTH_POLICY, DEFAULT_GROWTH_FACTOR, DEFAULT_GROWTH_CHUNK, DEFAULT_LOAD_FACTOR);
    }
    
    /**
     *  Creates a new hash map with capacity 11, a relative
     *  growth factor of 1.0, and a load factor of 75%.
     */
    public LongKeyChainedHashMap() {
        this(DEFAULT_CAPACITY,DEFAULT_GROWTH_POLICY, DEFAULT_GROWTH_FACTOR, DEFAULT_GROWTH_CHUNK, DEFAULT_LOAD_FACTOR);
    }



    // ---------------------------------------------------------------
    //      Hash table management
    // ---------------------------------------------------------------

    private void ensureCapacity(int elements) {
        if (elements >= expandAt) {
            int newcapacity;
            if (growthPolicy == GROWTH_POLICY_RELATIVE)
                newcapacity = (int)(data.length * (1.0 + growthFactor));
            else
                newcapacity = data.length + growthChunk;
            if (newcapacity*loadFactor < elements)
                newcapacity = (int)Math.round(((double)elements/loadFactor));
            //newcapacity = Primes.nextPrime(newcapacity);
            expandAt = (int)Math.round(loadFactor*newcapacity);

            Entry<E>[] newdata = makeEntryArray(newcapacity);

            //  re-hash
            for (int i = 0; i < data.length; i++) {
                Entry<E> e = data[i];
                while (e != null) {
                    int index = Math.abs(hash(e.key)) % newdata.length;
                    Entry<E> next = e.next;
                    e.next = newdata[index];
                    newdata[index] = e;
                    e = next;
                }
            }

            data = newdata;
        }
    }

	@SuppressWarnings("unchecked")
	private Entry<E>[] makeEntryArray(int newcapacity) {
		return (Entry<E>[]) new Entry[newcapacity];
	}

    private Entry<E> removeList(Entry<E> list, Entry<E> e) {
        if (list == e) {
            list = e.next;
            e.next = null;
            return list;
        }
        Entry<E> listStart = list;
        while (list.next != e)
            list = list.next;
        list.next = e.next;
        e.next = null;
        return listStart;
    }

    private Entry<E> searchList(Entry<E> list, long key) {
        while (list != null) {
            if (list.key == key)
                return list;
            list = list.next;
        }
        return null;
    }

    private Entry<E> getEntry(long key) {
        int index = Math.abs(hash(key)) % data.length;
        return searchList(data[index], key);
    }

    // ---------------------------------------------------------------
    //      Operations not supported by abstract implementation
    // ---------------------------------------------------------------

//    public LongSet keySet() {
//        if (keys == null)
//            keys = new KeySet();
//        return keys;
//    }

    public E put(long key, E value) {
        E result;
        int index = Math.abs(hash(key)) % data.length;
        Entry<E> e = searchList(data[index], key);
        if (e == null) {
            result = null;
            e = new Entry<E>(key, value);
            e.next = data[index];
            data[index] = e;
            //  Capacity is increased after insertion in order to
            //  avoid recalculation of index
            ensureCapacity(size+1);
            size++;
        } else {
            result = e.value;
            e.value = value;
        }
        return result;
    }

    public Collection<E> values() {
        if (values == null)
            values = new ValueCollection();
        return values;
    }

    private static class Entry<E> {
        long key;
        E value;
        Entry<E> next;

        Entry(long key, E value) {
            this.key = key;
            this.value = value;
        }

        public long getKey()
        { return key; }

        public E getValue()
        { return value; }

        @SuppressWarnings("unchecked")
		public boolean equals(Object obj) {
            if (!(obj instanceof Entry))
                return false;
            Entry<E> e = (Entry<E>) obj;
            Object eval = e.getValue();
            if (eval == null)
                return e.getKey() == key && value == null;
            return e.getKey() == key && e.getValue().equals(value);
        }
    }


//    public LongKeyMapIterator<E> entries() {
//        return new LongKeyMapIterator<E>() {
//            Entry<E> currEntry = null;
//            int nextList = nextList(0);
//            Entry<E> nextEntry = nextList == -1 ? null : data[nextList];
//
//            int nextList(int index) {
//                while (index < data.length && data[index] == null)
//                    index++;
//                return index < data.length ? index : -1;
//            }
//
//            public boolean hasNext() {
//                return nextEntry != null;
//            }
//
//            public void next() {
//                if (nextEntry == null)
//                    Exceptions.endOfIterator();
//                currEntry = nextEntry;
//
//                //  Find next
//                nextEntry = nextEntry.next;
//                if (nextEntry == null) {
//                    nextList = nextList(nextList+1);
//                    if (nextList != -1)
//                        nextEntry = data[nextList];
//                }
//            }
//
//            public long getKey() {
//                if (currEntry == null)
//                    Exceptions.noElementToGet();
//                return currEntry.getKey();
//            }
//
//            public E getValue() {
//                if (currEntry == null)
//                    Exceptions.noElementToGet();
//                return currEntry.getValue();
//            }
//
//            public void remove() {
//                if (currEntry == null)
//                    Exceptions.noElementToRemove();
//                 LongKeyChainedHashMap.this.remove(currEntry.getKey());
//                 currEntry = null;
//            }
//
//        };
//    }

//    private class KeySet extends AbstractLongSet {
//
//        public void clear()
//        { LongKeyChainedHashMap.this.clear(); }
//
//        public boolean contains(long v) {
//            return getEntry(v) != null;
//        }
//
//        public LongIterator iterator() {
//            return new LongIterator() {
//                Entry<E> currEntry = null;
//                int nextList = nextList(0);
//                Entry<E> nextEntry = nextList == -1 ? null : data[nextList];
//
//                int nextList(int index) {
//                    while (index < data.length && data[index] == null)
//                        index++;
//                    return index < data.length ? index : -1;
//                }
//
//                public boolean hasNext() {
//                    return nextEntry != null;
//                }
//
//                public long next() {
//                    if (nextEntry == null)
//                        throw new NoSuchElementException();
//                    currEntry = nextEntry;
//
//                    //  Find next
//                    nextEntry = nextEntry.next;
//                    if (nextEntry == null) {
//                        nextList = nextList(nextList+1);
//                        if (nextList != -1)
//                            nextEntry = data[nextList];
//                    }
//                    return currEntry.key;
//                }
//
//                public void remove() {
//                    if (currEntry == null)
//                    	throw new NoSuchElementException();
//                     LongKeyChainedHashMap.this.remove(currEntry.getKey());
//                     currEntry = null;
//                }
//            };
//        }
//
//        public boolean remove(long v) {
//            boolean result = containsKey(v);
//            if (result)
//                LongKeyChainedHashMap.this.remove(v);
//            return result;
//        }
//
//        public int size()
//        { return size; }
//
//    }


    private class ValueCollection extends AbstractCollection<E> {

        public void clear()
        { LongKeyChainedHashMap.this.clear(); }

        public boolean contains(Object v) {
            return containsValue(v);
        }

        public Iterator<E> iterator() {
            return new Iterator<E>() {
                Entry<E> currEntry = null;
                int nextList = nextList(0);
                Entry<E> nextEntry = nextList == -1 ? null : data[nextList];

                int nextList(int index) {
                    while (index < data.length && data[index] == null)
                        index++;
                    return index < data.length ? index : -1;
                }

                public boolean hasNext() {
                    return nextEntry != null;
                }

                public E next() {
                    if (nextEntry == null)
                        throw new NoSuchElementException();
                    currEntry = nextEntry;

                    //  Find next
                    nextEntry = nextEntry.next;
                    if (nextEntry == null) {
                        nextList = nextList(nextList+1);
                        if (nextList != -1)
                            nextEntry = data[nextList];
                    }
                    return currEntry.value;
                }

                public void remove() {
                    if (currEntry == null)
                    	throw new NoSuchElementException();
                     LongKeyChainedHashMap.this.remove(currEntry.getKey());
                     currEntry = null;
                }
            };
        }

        public int size()
        { return size; }

    }

    // ---------------------------------------------------------------
    //      Operations overwritten for efficiency
    // ---------------------------------------------------------------

    public void clear() {
        java.util.Arrays.fill(data, null);
        size = 0;
    }

    public boolean containsKey(long key) {
        Entry<E> e = getEntry(key);
        return e != null;
    }

    public boolean containsValue(Object value) {
        if (value == null) {
            for (int i = 0; i < data.length; i++) {
                Entry<E> e = data[i];
                while (e != null) {
                    if (e.value == null)
                        return true;
                    e = e.next;
                }
            }
        } else {
            for (int i = 0; i < data.length; i++) {
                Entry<E> e = data[i];
                while (e != null) {
                    if (value.equals(e.value))
                        return true;
                    e = e.next;
                }
            }
        }
        return false;
    }

    public E get(long key) {
        int index = Math.abs(hash(key)) % data.length;
        Entry<E> e = searchList(data[index], key);
        return e != null ? e.value : null;
    }

    public boolean isEmpty()
    { return size == 0; }

    public E remove(long key) {
        int index = Math.abs(hash(key)) % data.length;
        Entry<E> e = searchList(data[index], key);
        E value;
        if (e != null) {
            //  This can be improved to one iteration
            data[index] = removeList(data[index], e);
            value = e.value;
            size--;
        } else
            value = null;
        return value;
    }

    public int size()
    { return size; }


    public int hash(long v) {
        return (int)(v ^ (v >>> 32));
    }

}