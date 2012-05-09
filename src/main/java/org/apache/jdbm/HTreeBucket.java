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

package org.apache.jdbm;

import java.io.*;
import java.util.ArrayList;

/**
 * A bucket is a placeholder for multiple (key, value) pairs.  Buckets
 * are used to store collisions (same hash value) at all levels of an
 * H*tree.
 * <p/>
 * There are two types of buckets: leaf and non-leaf.
 * <p/>
 * Non-leaf buckets are buckets which hold collisions which happen
 * when the H*tree is not fully expanded.   Keys in a non-leaf buckets
 * can have different hash codes.  Non-leaf buckets are limited to an
 * arbitrary size.  When this limit is reached, the H*tree should create
 * a new HTreeDirectory node and distribute keys of the non-leaf buckets into
 * the newly created HTreeDirectory.
 * <p/>
 * A leaf bucket is a bucket which contains keys which all have
 * the same <code>hashCode()</code>.  Leaf buckets stand at the
 * bottom of an H*tree because the hashing algorithm cannot further
 * discriminate between different keys based on their hash code.
 *
 * @author Alex Boisvert
 */
final class HTreeBucket<K, V> {

    /**
     * The maximum number of elements (key, value) a non-leaf bucket
     * can contain.
     */
    public static final int OVERFLOW_SIZE = 16;


    /**
     * Depth of this bucket.
     */
    private byte _depth;


    /**
     * Keys and values in this bucket.  Keys are followed by values at KEYPOS+OVERFLOW_SIZE
     */
    private Object[] _keysAndValues;

    private byte size = 0;


    private final HTree<K, V> tree;


    /**
     * Public constructor for serialization.
     */
    public HTreeBucket(HTree<K, V> tree) {
        this.tree = tree;
    }


    /**
     * Construct a bucket with a given depth level.  Depth level is the
     * number of <code>HashDirectory</code> above this bucket.
     */
    public HTreeBucket(HTree<K, V> tree, byte level) {
        this.tree = tree;
        if (level > HTreeDirectory.MAX_DEPTH + 1) {
            throw new IllegalArgumentException(
                    "Cannot create bucket with depth > MAX_DEPTH+1. "
                            + "Depth=" + level);
        }
        _depth = level;
        _keysAndValues = new Object[OVERFLOW_SIZE * 2];
    }


    /**
     * Returns the number of elements contained in this bucket.
     */
    public int getElementCount() {
        return size;
    }


    /**
     * Returns whether or not this bucket is a "leaf bucket".
     */
    public boolean isLeaf() {
        return (_depth > HTreeDirectory.MAX_DEPTH);
    }


    /**
     * Returns true if bucket can accept at least one more element.
     */
    public boolean hasRoom() {
        if (isLeaf()) {
            return true;  // leaf buckets are never full
        } else {
            // non-leaf bucket
            return (size < OVERFLOW_SIZE);
        }
    }


    /**
     * Add an element (key, value) to this bucket.  If an existing element
     * has the same key, it is replaced silently.
     *
     * @return Object which was previously associated with the given key
     *         or <code>null</code> if no association existed.
     */
    public V addElement(K key, V value) {
        //find entry
        byte existing = -1;
        for (byte i = 0; i < size; i++) {
            if (key.equals(_keysAndValues[i])) {
                existing = i;
                break;
            }
        }

        if (existing != -1) {
            // replace existing element
            Object before = _keysAndValues[existing + OVERFLOW_SIZE];
            if (before instanceof BTreeLazyRecord) {
                BTreeLazyRecord<V> rec = (BTreeLazyRecord<V>) before;
                before = rec.get();
                rec.delete();
            }
            _keysAndValues[existing + OVERFLOW_SIZE] = value;
            return (V) before;
        } else {
            // add new (key, value) pair
            _keysAndValues[size] = key;
            _keysAndValues[size + OVERFLOW_SIZE] = value;
            size++;
            return null;
        }
    }


    /**
     * Remove an element, given a specific key.
     *
     * @param key Key of the element to remove
     * @return Removed element value, or <code>null</code> if not found
     */
    public V removeElement(K key) {
        //find entry
        byte existing = -1;
        for (byte i = 0; i < size; i++) {
            if (key.equals(_keysAndValues[i])) {
                existing = i;
                break;
            }
        }

        if (existing != -1) {
            Object o = _keysAndValues[existing + OVERFLOW_SIZE];
            if (o instanceof BTreeLazyRecord) {
                BTreeLazyRecord<V> rec = (BTreeLazyRecord<V>) o;
                o = rec.get();
                rec.delete();
            }


            //move last element to existing
            size--;
            _keysAndValues[existing] = _keysAndValues[size];
            _keysAndValues[existing + OVERFLOW_SIZE] = _keysAndValues[size + OVERFLOW_SIZE];

            //and unset last element
            _keysAndValues[size] = null;
            _keysAndValues[size + OVERFLOW_SIZE] = null;


            return (V) o;
        } else {
            // not found
            return null;
        }
    }


    /**
     * Returns the value associated with a given key.  If the given key
     * is not found in this bucket, returns <code>null</code>.
     */
    public V getValue(K key) {
        //find entry
        byte existing = -1;
        for (byte i = 0; i < size; i++) {
            if (key.equals(_keysAndValues[i])) {
                existing = i;
                break;
            }
        }

        if (existing != -1) {
            Object o = _keysAndValues[existing + OVERFLOW_SIZE];
            if (o instanceof BTreeLazyRecord)
                return ((BTreeLazyRecord<V>) o).get();
            else
                return (V) o;
        } else {
            // key not found
            return null;
        }
    }


    /**
     * Obtain keys contained in this buckets.  Keys are ordered to match
     * their values, which be be obtained by calling <code>getValues()</code>.
     * <p/>
     * As an optimization, the Vector returned is the instance member
     * of this class.  Please don't modify outside the scope of this class.
     */
    ArrayList<K> getKeys() {
        ArrayList<K> ret = new ArrayList<K>();
        for (byte i = 0; i < size; i++) {
            ret.add((K) _keysAndValues[i]);
        }
        return ret;
    }


    /**
     * Obtain values contained in this buckets.  Values are ordered to match
     * their keys, which be be obtained by calling <code>getKeys()</code>.
     * <p/>
     * As an optimization, the Vector returned is the instance member
     * of this class.  Please don't modify outside the scope of this class.
     */
    ArrayList<V> getValues() {
        ArrayList<V> ret = new ArrayList<V>();
        for (byte i = 0; i < size; i++) {
            ret.add((V) _keysAndValues[i + OVERFLOW_SIZE]);
        }
        return ret;

    }


    public void writeExternal(DataOutput out)
            throws IOException {
        out.write(_depth);
        out.write(size);


        DataInputOutput out3 = tree.writeBufferCache.getAndSet(null);
        if (out3 == null)
            out3 = new DataInputOutput();
        else
            out3.reset();

        Serializer keySerializer = tree.keySerializer != null ? tree.keySerializer : tree.getRecordManager().defaultSerializer();
        for (byte i = 0; i < size; i++) {
            out3.reset();
            keySerializer.serialize(out3, _keysAndValues[i]);
            LongPacker.packInt(out, out3.getPos());
            out.write(out3.getBuf(), 0, out3.getPos());

        }

        //write values
        if(tree.hasValues()){
            Serializer valSerializer = tree.valueSerializer != null ? tree.valueSerializer : tree.getRecordManager().defaultSerializer();

            for (byte i = 0; i < size; i++) {
                Object value = _keysAndValues[i + OVERFLOW_SIZE];
                if (value == null) {
                    out.write(BTreeLazyRecord.NULL);
                } else if (value instanceof BTreeLazyRecord) {
                    out.write(BTreeLazyRecord.LAZY_RECORD);
                    LongPacker.packLong(out, ((BTreeLazyRecord) value).recid);
                } else {
                    //transform to byte array
                    out3.reset();
                    valSerializer.serialize(out3, value);

                    if (out3.getPos() > BTreeLazyRecord.MAX_INTREE_RECORD_SIZE) {
                        //store as separate record
                        long recid = tree.getRecordManager().insert(out3.toByteArray(), BTreeLazyRecord.FAKE_SERIALIZER,true);
                        out.write(BTreeLazyRecord.LAZY_RECORD);
                        LongPacker.packLong(out, recid);
                    } else {
                        out.write(out3.getPos());
                        out.write(out3.getBuf(), 0, out3.getPos());
                    }
                }
            }
        }
        tree.writeBufferCache.set(out3);

    }


    public void readExternal(DataInputOutput in) throws IOException, ClassNotFoundException {
        _depth = in.readByte();
        size = in.readByte();

        //read keys
        Serializer keySerializer = tree.keySerializer != null ? tree.keySerializer : tree.getRecordManager().defaultSerializer();
        _keysAndValues = (K[]) new Object[OVERFLOW_SIZE * 2];
        for (byte i = 0; i < size; i++) {
            int expectedSize = LongPacker.unpackInt(in);
            K key = (K) BTreeLazyRecord.fastDeser(in, keySerializer, expectedSize);
            _keysAndValues[i] = key;
        }

        //read values
        if(tree.hasValues()){
            Serializer<V> valSerializer = tree.valueSerializer != null ? tree.valueSerializer : (Serializer<V>) tree.getRecordManager().defaultSerializer();
            for (byte i = 0; i < size; i++) {
                int header = in.readUnsignedByte();
                if (header == BTreeLazyRecord.NULL) {
                    _keysAndValues[i + OVERFLOW_SIZE] = null;
                } else if (header == BTreeLazyRecord.LAZY_RECORD) {
                    long recid = LongPacker.unpackLong(in);
                    _keysAndValues[i + OVERFLOW_SIZE] = (new BTreeLazyRecord(tree.getRecordManager(), recid, valSerializer));
                } else {
                    _keysAndValues[i + OVERFLOW_SIZE] = BTreeLazyRecord.fastDeser(in, valSerializer, header);
                }
            }
        }else{
            for (byte i = 0; i < size; i++) {
                if(_keysAndValues[i]!=null)
                    _keysAndValues[i+OVERFLOW_SIZE] = Utils.EMPTY_STRING;
            }
        }
    }
}


