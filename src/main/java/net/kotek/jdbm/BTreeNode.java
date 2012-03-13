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


package net.kotek.jdbm;

import java.io.*;
import java.util.ConcurrentModificationException;
import java.util.List;

/**
 * Node of a BTree.
 * <p/>
 * The node contains a number of key-value pairs.  Keys are ordered to allow
 * dichotomic search. If value is too big, it is stored in separate record
 * and only recid reference is stored
 * <p/>
 * If the node is a leaf node, the keys and values are user-defined and
 * represent entries inserted by the user.
 * <p/>
 * If the node is non-leaf, each key represents the greatest key in the
 * underlying BTreeNode and the values are recids pointing to the children BTreeNodes.
 * The only exception is the rightmost BTreeNode, which is considered to have an
 * "infinite" key value, meaning that any insert will be to the left of this
 * pseudo-key
 *
 * @author Alex Boisvert
 * @author Jan Kotek
 */
final class BTreeNode<K, V>
        implements Serializer<BTreeNode<K, V>> {

    private static final boolean DEBUG = false;


    /**
     * Parent B+Tree.
     */
    transient BTree<K, V> _btree;


    /**
     * This BTreeNode's record ID in the DB.
     */
    protected transient long _recid;


    /**
     * Flag indicating if this is a leaf BTreeNode.
     */
    protected boolean _isLeaf;


    /**
     * Keys of children nodes
     */
    protected K[] _keys;


    /**
     * Values associated with keys.  (Only valid if leaf node)
     */
    protected Object[] _values;


    /**
     * Children nodes (recids) associated with keys.  (Only valid if non-leaf node)
     */
    protected long[] _children;


    /**
     * Index of first used item at the node
     */
    protected byte _first;


    /**
     * Previous leaf node (only if this node is a leaf)
     */
    protected long _previous;


    /**
     * Next leaf node (only if this node is a leaf)
     */
    protected long _next;

    /**
     * Return the B+Tree that is the owner of this {@link BTreeNode}.
     */
    public BTree<K, V> getBTree() {
        return _btree;
    }

    /**
     * No-argument constructor used by serialization.
     */
    public BTreeNode() {
        // empty
    }


    /**
     * Root node overflow constructor
     */
    @SuppressWarnings("unchecked")
    BTreeNode(BTree<K, V> btree, BTreeNode<K, V> root, BTreeNode<K, V> overflow)
            throws IOException {
        _btree = btree;

        _isLeaf = false;

        _first = BTree.DEFAULT_SIZE - 2;

        _keys = (K[]) new Object[BTree.DEFAULT_SIZE];
        _keys[BTree.DEFAULT_SIZE - 2] = overflow.getLargestKey();
        _keys[BTree.DEFAULT_SIZE - 1] = root.getLargestKey();

        _children = new long[BTree.DEFAULT_SIZE];
        _children[BTree.DEFAULT_SIZE - 2] = overflow._recid;
        _children[BTree.DEFAULT_SIZE - 1] = root._recid;

        _recid = _btree._db.insert(this, this,false);
    }


    /**
     * Root node (first insert) constructor.
     */
    @SuppressWarnings("unchecked")
    BTreeNode(BTree<K, V> btree, K key, V value)
            throws IOException {
        _btree = btree;

        _isLeaf = true;

        _first = BTree.DEFAULT_SIZE - 2;

        _keys = (K[]) new Object[BTree.DEFAULT_SIZE];
        _keys[BTree.DEFAULT_SIZE - 2] = key;
        _keys[BTree.DEFAULT_SIZE - 1] = null;  // I am the root BTreeNode for now

        _values = new Object[BTree.DEFAULT_SIZE];
        _values[BTree.DEFAULT_SIZE - 2] = value;
        _values[BTree.DEFAULT_SIZE - 1] = null;  // I am the root BTreeNode for now

        _recid = _btree._db.insert(this, this,false);
    }


    /**
     * Overflow node constructor.  Creates an empty BTreeNode.
     */
    @SuppressWarnings("unchecked")
    BTreeNode(BTree<K, V> btree, boolean isLeaf){
        _btree = btree;

        _isLeaf = isLeaf;

        // node will initially be half-full
        _first = BTree.DEFAULT_SIZE / 2;

        _keys = (K[]) new Object[BTree.DEFAULT_SIZE];
        if (isLeaf) {
            _values = new Object[BTree.DEFAULT_SIZE];
        } else {
            _children = new long[BTree.DEFAULT_SIZE];
        }

        try{
            _recid = _btree._db.insert(this, this,false);
        }catch(IOException e ){
            throw new IOError(e);
        }
    }


    /**
     * Get largest key under this BTreeNode.  Null is considered to be the
     * greatest possible key.
     */
    K getLargestKey() {
        return _keys[BTree.DEFAULT_SIZE - 1];
    }


    /**
     * Return true if BTreeNode is empty.
     */
    boolean isEmpty() {
        if (_isLeaf) {
            return (_first == _values.length - 1);
        } else {
            return (_first == _children.length - 1);
        }
    }


    /**
     * Return true if BTreeNode is full.
     */
    boolean isFull() {
        return (_first == 0);
    }


    /**
     * Find the object associated with the given key.
     *
     * @param height Height of the current BTreeNode (zero is leaf node)
     * @param key    The key
     * @return TupleBrowser positionned just before the given key, or before
     *         next greater key if key isn't found.
     */
    BTree.BTreeTupleBrowser<K, V> find(int height, final K key, final boolean inclusive)
            throws IOException {
        byte index = findChildren(key,inclusive);

        height -= 1;

        if (height == 0) {
            // leaf node
            return new Browser<K, V>(this, index);
        } else {
            // non-leaf node
            BTreeNode<K, V> child = loadNode(_children[index]);
            return child.find(height, key,inclusive);
        }
    }


    /**
     * Find value associated with the given key.
     *
     * @param height Height of the current BTreeNode (zero is leaf node)
     * @param key    The key
     * @return TupleBrowser positionned just before the given key, or before
     *         next greater key if key isn't found.
     */
    V findValue(int height, K key)
            throws IOException {
        byte index = findChildren(key,true);

        height -= 1;

        if (height == 0) {

            K key2 = _keys[index];
//          // get returns the matching key or the next ordered key, so we must
//          // check if we have an exact match
            if (key2 == null || compare(key, key2) != 0)
                return null;

            // leaf node
            if (_values[index] instanceof BTreeLazyRecord)
                return ((BTreeLazyRecord<V>) _values[index]).get();
            else
                return (V) _values[index];


        } else {
            // non-leaf node
            BTreeNode<K, V> child = loadNode(_children[index]);
            return child.findValue(height, key);
        }
    }

    /**
     * Find first entry and return a browser positioned before it.
     *
     * @return TupleBrowser positionned just before the first entry.
     */
    BTree.BTreeTupleBrowser<K, V> findFirst()
            throws IOException {
        if (_isLeaf) {
            return new Browser<K, V>(this, _first);
        } else {
            BTreeNode<K, V> child = loadNode(_children[_first]);
            return child.findFirst();
        }
    }

    /**
     * Deletes this BTreeNode and all children nodes from the record manager
     */
    void delete()
            throws IOException {
        if (_isLeaf) {
            if (_next != 0) {
                BTreeNode<K, V> nextNode = loadNode(_next);
                if (nextNode._previous == _recid) { // this consistency check can be removed in production code
                    nextNode._previous = _previous;
                    _btree._db.update(nextNode._recid, nextNode, nextNode);
                } else {
                    throw new Error("Inconsistent data in BTree");
                }
            }
            if (_previous != 0) {
                BTreeNode<K, V> previousNode = loadNode(_previous);
                if (previousNode._next != _recid) { // this consistency check can be removed in production code
                    previousNode._next = _next;
                    _btree._db.update(previousNode._recid, previousNode, previousNode);
                } else {
                    throw new Error("Inconsistent data in BTree");
                }
            }
        } else {
            int left = _first;
            int right = BTree.DEFAULT_SIZE - 1;

            for (int i = left; i <= right; i++) {
                BTreeNode<K, V> childNode = loadNode(_children[i]);
                childNode.delete();
            }
        }

        _btree._db.delete(_recid);
    }

    /**
     * Insert the given key and value.
     * <p/>
     * Since the Btree does not support duplicate entries, the caller must
     * specify whether to replace the existing value.
     *
     * @param height  Height of the current BTreeNode (zero is leaf node)
     * @param key     Insert key
     * @param value   Insert value
     * @param replace Set to true to replace the existing value, if one exists.
     * @return Insertion result containing existing value OR a BTreeNode if the key
     *         was inserted and provoked a BTreeNode overflow.
     */
    InsertResult<K, V> insert(int height, K key, final V value, final boolean replace)
            throws IOException {
        InsertResult<K, V> result;
        long overflow;

        final byte index = findChildren(key,true);

        height -= 1;
        if (height == 0) {

            //reuse InsertResult instance to avoid GC trashing on massive inserts
            result = _btree.insertResultReuse;
            _btree.insertResultReuse = null;
            if (result == null)
                result = new InsertResult<K, V>();

            // inserting on a leaf BTreeNode
            overflow = -1;
            if (DEBUG) {
                System.out.println("BTreeNode.insert() Insert on leaf node key=" + key
                        + " value=" + value + " index=" + index);
            }
            if (compare(_keys[index], key) == 0) {
                // key already exists
                if (DEBUG) {
                    System.out.println("BTreeNode.insert() Key already exists.");
                }
                boolean isLazyRecord = _values[index] instanceof BTreeLazyRecord;
                if (isLazyRecord)
                    result._existing = ((BTreeLazyRecord<V>) _values[index]).get();
                else
                    result._existing = (V) _values[index];
                if (replace) {
                    //remove old lazy record if necesarry
                    if (isLazyRecord)
                        ((BTreeLazyRecord) _values[index]).delete();
                    _values[index] = value;
                    _btree._db.update(_recid, this, this);
                }
                // return the existing key
                return result;
            }
        } else {
            // non-leaf BTreeNode
            BTreeNode<K, V> child = loadNode(_children[index]);
            result = child.insert(height, key, value, replace);

            if (result._existing != null) {
                // return existing key, if any.
                return result;
            }

            if (result._overflow == null) {
                // no overflow means we're done with insertion
                return result;
            }

            // there was an overflow, we need to insert the overflow node on this BTreeNode
            if (DEBUG) {
                System.out.println("BTreeNode.insert() Overflow node: " + result._overflow._recid);
            }
            key = result._overflow.getLargestKey();
            overflow = result._overflow._recid;

            // update child's largest key
            _keys[index] = child.getLargestKey();

            // clean result so we can reuse it
            result._overflow = null;
        }

        // if we get here, we need to insert a new entry on the BTreeNode before _children[ index ]
        if (!isFull()) {
            if (height == 0) {
                insertEntry(this, index - 1, key, value);
            } else {
                insertChild(this, index - 1, key, overflow);
            }
            _btree._db.update(_recid, this, this);
            return result;
        }

        // node is full, we must divide the node
        final byte half = BTree.DEFAULT_SIZE >> 1;
        BTreeNode<K, V> newNode = new BTreeNode<K, V>(_btree, _isLeaf);
        if (index < half) {
            // move lower-half of entries to overflow node, including new entry
            if (DEBUG) {
                System.out.println("BTreeNode.insert() move lower-half of entries to overflow BTreeNode, including new entry.");
            }
            if (height == 0) {
                copyEntries(this, 0, newNode, half, index);
                setEntry(newNode, half + index, key, value);
                copyEntries(this, index, newNode, half + index + 1, half - index - 1);
            } else {
                copyChildren(this, 0, newNode, half, index);
                setChild(newNode, half + index, key, overflow);
                copyChildren(this, index, newNode, half + index + 1, half - index - 1);
            }
        } else {
            // move lower-half of entries to overflow node, new entry stays on this node
            if (DEBUG) {
                System.out.println("BTreeNode.insert() move lower-half of entries to overflow BTreeNode. New entry stays");
            }
            if (height == 0) {
                copyEntries(this, 0, newNode, half, half);
                copyEntries(this, half, this, half - 1, index - half);
                setEntry(this, index - 1, key, value);
            } else {
                copyChildren(this, 0, newNode, half, half);
                copyChildren(this, half, this, half - 1, index - half);
                setChild(this, index - 1, key, overflow);
            }
        }

        _first = half - 1;

        // nullify lower half of entries
        for (int i = 0; i < _first; i++) {
            if (height == 0) {
                setEntry(this, i, null, null);
            } else {
                setChild(this, i, null, -1);
            }
        }

        if (_isLeaf) {
            // link newly created node
            newNode._previous = _previous;
            newNode._next = _recid;
            if (_previous != 0) {
                BTreeNode<K, V> previous = loadNode(_previous);
                previous._next = newNode._recid;
                _btree._db.update(_previous, previous, this);

            }
            _previous = newNode._recid;
        }

        _btree._db.update(_recid, this, this);
        _btree._db.update(newNode._recid, newNode, this);

        result._overflow = newNode;
        return result;
    }


    /**
     * Remove the entry associated with the given key.
     *
     * @param height Height of the current BTreeNode (zero is leaf node)
     * @param key    Removal key
     * @return Remove result object
     */
    RemoveResult<K, V> remove(int height, K key)
            throws IOException {
        RemoveResult<K, V> result;

        int half = BTree.DEFAULT_SIZE / 2;
        byte index = findChildren(key,true);

        height -= 1;
        if (height == 0) {
            // remove leaf entry
            if (compare(_keys[index], key) != 0) {
                throw new IllegalArgumentException("Key not found: " + key);
            }
            result = new RemoveResult<K, V>();

            if (_values[index] instanceof BTreeLazyRecord) {
                BTreeLazyRecord<V> r = (BTreeLazyRecord<V>) _values[index];
                result._value = r.get();
                r.delete();
            } else {
                result._value = (V) _values[index];
            }
            removeEntry(this, index);

            // update this node
            _btree._db.update(_recid, this, this);

        } else {
            // recurse into Btree to remove entry on a children node
            BTreeNode<K, V> child = loadNode(_children[index]);
            result = child.remove(height, key);

            // update children
            _keys[index] = child.getLargestKey();
            _btree._db.update(_recid, this, this);

            if (result._underflow) {
                // underflow occured
                if (child._first != half + 1) {
                    throw new IllegalStateException("Error during underflow [1]");
                }
                if (index < _children.length - 1) {
                    // exists greater brother node
                    BTreeNode<K, V> brother = loadNode(_children[index + 1]);
                    int bfirst = brother._first;
                    if (bfirst < half) {
                        // steal entries from "brother" node
                        int steal = (half - bfirst + 1) / 2;
                        brother._first += steal;
                        child._first -= steal;
                        if (child._isLeaf) {
                            copyEntries(child, half + 1, child, half + 1 - steal, half - 1);
                            copyEntries(brother, bfirst, child, 2 * half - steal, steal);
                        } else {
                            copyChildren(child, half + 1, child, half + 1 - steal, half - 1);
                            copyChildren(brother, bfirst, child, 2 * half - steal, steal);
                        }

                        for (int i = bfirst; i < bfirst + steal; i++) {
                            if (brother._isLeaf) {
                                setEntry(brother, i, null, null);
                            } else {
                                setChild(brother, i, null, -1);
                            }
                        }

                        // update child's largest key
                        _keys[index] = child.getLargestKey();

                        // no change in previous/next node

                        // update nodes
                        _btree._db.update(_recid, this, this);
                        _btree._db.update(brother._recid, brother, this);
                        _btree._db.update(child._recid, child, this);

                    } else {
                        // move all entries from node "child" to "brother"
                        if (brother._first != half) {
                            throw new IllegalStateException("Error during underflow [2]");
                        }

                        brother._first = 1;
                        if (child._isLeaf) {
                            copyEntries(child, half + 1, brother, 1, half - 1);
                        } else {
                            copyChildren(child, half + 1, brother, 1, half - 1);
                        }
                        _btree._db.update(brother._recid, brother, this);


                        // remove "child" from current node
                        if (_isLeaf) {
                            copyEntries(this, _first, this, _first + 1, index - _first);
                            setEntry(this, _first, null, null);
                        } else {
                            copyChildren(this, _first, this, _first + 1, index - _first);
                            setChild(this, _first, null, -1);
                        }
                        _first += 1;
                        _btree._db.update(_recid, this, this);

                        // re-link previous and next nodes
                        if (child._previous != 0) {
                            BTreeNode<K, V> prev = loadNode(child._previous);
                            prev._next = child._next;
                            _btree._db.update(prev._recid, prev, this);
                        }
                        if (child._next != 0) {
                            BTreeNode<K, V> next = loadNode(child._next);
                            next._previous = child._previous;
                            _btree._db.update(next._recid, next, this);

                        }

                        // delete "child" node
                        _btree._db.delete(child._recid);
                    }
                } else {
                    // node "brother" is before "child"
                    BTreeNode<K, V> brother = loadNode(_children[index - 1]);
                    int bfirst = brother._first;
                    if (bfirst < half) {
                        // steal entries from "brother" node
                        int steal = (half - bfirst + 1) / 2;
                        brother._first += steal;
                        child._first -= steal;
                        if (child._isLeaf) {
                            copyEntries(brother, 2 * half - steal, child,
                                    half + 1 - steal, steal);
                            copyEntries(brother, bfirst, brother,
                                    bfirst + steal, 2 * half - bfirst - steal);
                        } else {
                            copyChildren(brother, 2 * half - steal, child,
                                    half + 1 - steal, steal);
                            copyChildren(brother, bfirst, brother,
                                    bfirst + steal, 2 * half - bfirst - steal);
                        }

                        for (int i = bfirst; i < bfirst + steal; i++) {
                            if (brother._isLeaf) {
                                setEntry(brother, i, null, null);
                            } else {
                                setChild(brother, i, null, -1);
                            }
                        }

                        // update brother's largest key
                        _keys[index - 1] = brother.getLargestKey();

                        // no change in previous/next node

                        // update nodes
                        _btree._db.update(_recid, this, this);
                        _btree._db.update(brother._recid, brother, this);
                        _btree._db.update(child._recid, child, this);

                    } else {
                        // move all entries from node "brother" to "child"
                        if (brother._first != half) {
                            throw new IllegalStateException("Error during underflow [3]");
                        }

                        child._first = 1;
                        if (child._isLeaf) {
                            copyEntries(brother, half, child, 1, half);
                        } else {
                            copyChildren(brother, half, child, 1, half);
                        }
                        _btree._db.update(child._recid, child, this);

                        // remove "brother" from current node
                        if (_isLeaf) {
                            copyEntries(this, _first, this, _first + 1, index - 1 - _first);
                            setEntry(this, _first, null, null);
                        } else {
                            copyChildren(this, _first, this, _first + 1, index - 1 - _first);
                            setChild(this, _first, null, -1);
                        }
                        _first += 1;
                        _btree._db.update(_recid, this, this);

                        // re-link previous and next nodes
                        if (brother._previous != 0) {
                            BTreeNode<K, V> prev = loadNode(brother._previous);
                            prev._next = brother._next;
                            _btree._db.update(prev._recid, prev, this);
                        }
                        if (brother._next != 0) {
                            BTreeNode<K, V> next = loadNode(brother._next);
                            next._previous = brother._previous;
                            _btree._db.update(next._recid, next, this);
                        }

                        // delete "brother" node
                        _btree._db.delete(brother._recid);
                    }
                }
            }
        }

        // underflow if node is more than half-empty
        result._underflow = _first > half;

        return result;
    }


    /**
     * Find the first children node with a key equal or greater than the given
     * key.
     *
     * @return index of first children with equal or greater key.
     */
    private byte findChildren(final K key, final boolean inclusive) {
        int left = _first;
        int right = BTree.DEFAULT_SIZE - 1;
        int middle;
        final int D = inclusive?0:1;

        // binary search
        while (true) {
            middle = (left + right) / 2;
            if (compare(_keys[middle], key) < D) {
                left = middle + 1;
            } else {
                right = middle;
            }
            if (left >= right) {
                return (byte) right;
            }
        }
    }


    /**
     * Insert entry at given position.
     */
    private static <K, V> void insertEntry(BTreeNode<K, V> node, int index,
                                           K key, V value) {
        K[] keys = node._keys;
        Object[] values = node._values;
        int start = node._first;
        int count = index - node._first + 1;

        // shift entries to the left
        System.arraycopy(keys, start, keys, start - 1, count);
        System.arraycopy(values, start, values, start - 1, count);
        node._first -= 1;
        keys[index] = key;
        values[index] = value;
    }


    /**
     * Insert child at given position.
     */
    private static <K, V> void insertChild(BTreeNode<K, V> node, int index,
                                           K key, long child) {
        K[] keys = node._keys;
        long[] children = node._children;
        int start = node._first;
        int count = index - node._first + 1;

        // shift entries to the left
        System.arraycopy(keys, start, keys, start - 1, count);
        System.arraycopy(children, start, children, start - 1, count);
        node._first -= 1;
        keys[index] = key;
        children[index] = child;
    }

    /**
     * Remove entry at given position.
     */
    private static <K, V> void removeEntry(BTreeNode<K, V> node, int index) {
        K[] keys = node._keys;
        Object[] values = node._values;
        int start = node._first;
        int count = index - node._first;

        System.arraycopy(keys, start, keys, start + 1, count);
        keys[start] = null;
        System.arraycopy(values, start, values, start + 1, count);
        values[start] = null;
        node._first++;
    }


    /**
     * Set the entry at the given index.
     */
    private static <K, V> void setEntry(BTreeNode<K, V> node, int index, K key, V value) {
        node._keys[index] = key;
        node._values[index] = value;
    }


    /**
     * Set the child BTreeNode recid at the given index.
     */
    private static <K, V> void setChild(BTreeNode<K, V> node, int index, K key, long recid) {
        node._keys[index] = key;
        node._children[index] = recid;
    }


    /**
     * Copy entries between two nodes
     */
    private static <K, V> void copyEntries(BTreeNode<K, V> source, int indexSource,
                                           BTreeNode<K, V> dest, int indexDest, int count) {
        System.arraycopy(source._keys, indexSource, dest._keys, indexDest, count);
        System.arraycopy(source._values, indexSource, dest._values, indexDest, count);
    }


    /**
     * Copy child node recids between two nodes
     */
    private static <K, V> void copyChildren(BTreeNode<K, V> source, int indexSource,
                                            BTreeNode<K, V> dest, int indexDest, int count) {
        System.arraycopy(source._keys, indexSource, dest._keys, indexDest, count);
        System.arraycopy(source._children, indexSource, dest._children, indexDest, count);
    }


    /**
     * Load the node at the given recid.
     */
    private BTreeNode<K, V> loadNode(long recid)
            throws IOException {
        BTreeNode<K, V> child = _btree._db.fetch(recid, this);
        child._recid = recid;
        child._btree = _btree;
        return child;
    }


    private final int compare(final K value1, final K value2) {
        if (value1 == null) {
            return 1;
        }
        if (value2 == null) {
            return -1;
        }

        if (_btree._comparator == null) {
            return ((Comparable) value1).compareTo(value2);
        } else {
            return _btree._comparator.compare(value1, value2);
        }

    }

    /**
     * Dump the structure of the tree on the screen.  This is used for debugging
     * purposes only.
     */
    private void dump(int height) {
        String prefix = "";
        for (int i = 0; i < height; i++) {
            prefix += "    ";
        }
        System.out.println(prefix + "-------------------------------------- BTreeNode recid=" + _recid);
        System.out.println(prefix + "first=" + _first);
        for (int i = 0; i < BTree.DEFAULT_SIZE; i++) {
            if (_isLeaf) {
                System.out.println(prefix + "BTreeNode [" + i + "] " + _keys[i] + " " + _values[i]);
            } else {
                System.out.println(prefix + "BTreeNode [" + i + "] " + _keys[i] + " " + _children[i]);
            }
        }
        System.out.println(prefix + "--------------------------------------");
    }


    /**
     * Recursively dump the state of the BTree on screen.  This is used for
     * debugging purposes only.
     */
    void dumpRecursive(int height, int level)
            throws IOException {
        height -= 1;
        level += 1;
        if (height > 0) {
            for (byte i = _first; i < BTree.DEFAULT_SIZE; i++) {
                if (_keys[i] == null) break;
                BTreeNode<K, V> child = loadNode(_children[i]);
                child.dump(level);
                child.dumpRecursive(height, level);
            }
        }
    }

    /**
     * Deserialize the content of an object from a byte array.
     */
    @SuppressWarnings("unchecked")
    public BTreeNode<K, V> deserialize(DataInput ois2)
            throws IOException {
        DataInputOutput ois = (DataInputOutput) ois2;


        BTreeNode<K, V> node = new BTreeNode<K, V>();

        switch (ois.readUnsignedByte()) {
            case SerializationHeader.BTREE_NODE_LEAF:
                node._isLeaf = true;
                break;
            case SerializationHeader.BTREE_NODE_NONLEAF:
                node._isLeaf = false;
                break;
            default:
                throw new InternalError("wrong BTreeNode header");
        }

        if (node._isLeaf) {
            node._previous = LongPacker.unpackLong(ois);
            node._next = LongPacker.unpackLong(ois);
        }


        node._first = ois.readByte();

        if (!node._isLeaf) {
            node._children = new long[BTree.DEFAULT_SIZE];
            for (int i = node._first; i < BTree.DEFAULT_SIZE; i++) {
                node._children[i] = LongPacker.unpackLong(ois);
            }
        }

        if (!_btree.loadValues)
            return node;

        try {

            node._keys = readKeys(ois, node._first);

        } catch (ClassNotFoundException except) {
            throw new IOException(except.getMessage());
        }

        if (node._isLeaf) {

            try {
                readValues(ois, node);
            } catch (ClassNotFoundException except) {
                throw new IOException(except);
            }
        }

        return node;

    }


    /**
     * Serialize the content of an object into a byte array.
     *
     * @param obj Object to serialize
     * @return a byte array representing the object's state
     */
    public void serialize(DataOutput oos, BTreeNode<K, V> obj)
            throws IOException {


        // note:  It is assumed that BTreeNode instance doing the serialization is the parent
        // of the BTreeNode object being serialized.

        BTreeNode<K, V> node = obj;

        oos.writeByte(node._isLeaf ? SerializationHeader.BTREE_NODE_LEAF : SerializationHeader.BTREE_NODE_NONLEAF);
        if (node._isLeaf) {
            LongPacker.packLong(oos, node._previous);
            LongPacker.packLong(oos, node._next);
        }

        oos.write(node._first);

        if (!node._isLeaf) {
            for (int i = node._first; i < BTree.DEFAULT_SIZE; i++) {
                LongPacker.packLong(oos, node._children[i]);
            }
        }

        writeKeys(oos, node._keys, node._first);

        if (node._isLeaf && _btree.hasValues()) {
            writeValues(oos, node);
        }
    }


    private void readValues(DataInputOutput ois, BTreeNode<K, V> node) throws IOException, ClassNotFoundException {
        node._values = new Object[BTree.DEFAULT_SIZE];
        if(_btree.hasValues()){
            Serializer<V> serializer = _btree.valueSerializer != null ? _btree.valueSerializer : (Serializer<V>) _btree.getRecordManager().defaultSerializer();
            for (int i = node._first; i < BTree.DEFAULT_SIZE; i++) {
                int header = ois.readUnsignedByte();
                if (header == BTreeLazyRecord.NULL) {
                    node._values[i] = null;
                } else if (header == BTreeLazyRecord.LAZY_RECORD) {
                    long recid = LongPacker.unpackLong(ois);
                    node._values[i] = new BTreeLazyRecord(_btree._db, recid, serializer);
                } else {
                    node._values[i] = BTreeLazyRecord.fastDeser(ois, serializer, header);
                }
            }
        }else{
            //create fake values
            for (int i = node._first; i < BTree.DEFAULT_SIZE; i++) {
                if(node._keys[i]!=null)
                    node._values[i] = Utils.EMPTY_STRING;
            }
        }
    }


    private void writeValues(DataOutput oos, BTreeNode<K, V> node) throws IOException {


        DataInputOutput output = null;
        Serializer serializer = _btree.valueSerializer != null ? _btree.valueSerializer : _btree.getRecordManager().defaultSerializer();
        for (int i = node._first; i < BTree.DEFAULT_SIZE; i++) {
            if (node._values[i] instanceof BTreeLazyRecord) {
                oos.write(BTreeLazyRecord.LAZY_RECORD);
                LongPacker.packLong(oos, ((BTreeLazyRecord) node._values[i]).recid);
            } else if (node._values[i] != null) {
                if (output == null) {
                    output = new DataInputOutput();
                } else {
                    output.reset();
                }

                serializer.serialize(output, node._values[i]);

                if (output.getPos() > BTreeLazyRecord.MAX_INTREE_RECORD_SIZE) {
                    //write as separate record
                    long recid = _btree._db.insert(output.toByteArray(), BTreeLazyRecord.FAKE_SERIALIZER,true);
                    oos.write(BTreeLazyRecord.LAZY_RECORD);
                    LongPacker.packLong(oos, recid);
                } else {
                    //write as part of btree
                    oos.write(output.getPos());
                    oos.write(output.getBuf(), 0, output.getPos());
                }
            } else {
                oos.write(BTreeLazyRecord.NULL);
            }
        }
    }


    private static final int ALL_NULL = 0;
    private static final int ALL_INTEGERS = 1 << 5;
    private static final int ALL_INTEGERS_NEGATIVE = 2 << 5;
    private static final int ALL_LONGS = 3 << 5;
    private static final int ALL_LONGS_NEGATIVE = 4 << 5;
    private static final int ALL_STRINGS = 5 << 5;
    private static final int ALL_OTHER = 6 << 5;


    private K[] readKeys(DataInput ois, final int firstUse) throws IOException, ClassNotFoundException {
        Object[] ret = new Object[BTree.DEFAULT_SIZE];
        final int type = ois.readUnsignedByte();
        if (type == ALL_NULL) {
            return (K[]) ret;
        } else if (type == ALL_INTEGERS || type == ALL_INTEGERS_NEGATIVE) {
            long first = LongPacker.unpackLong(ois);
            if (type == ALL_INTEGERS_NEGATIVE)
                first = -first;
            ret[firstUse] = Integer.valueOf((int) first);
            for (int i = firstUse + 1; i < BTree.DEFAULT_SIZE; i++) {
//				ret[i] = Serialization.readObject(ois);
                long v = LongPacker.unpackLong(ois);
                if (v == 0) continue; //null
                v = v + first;
                ret[i] = Integer.valueOf((int) v);
                first = v;
            }
            return (K[]) ret;
        } else if (type == ALL_LONGS || type == ALL_LONGS_NEGATIVE) {
            long first = LongPacker.unpackLong(ois);
            if (type == ALL_LONGS_NEGATIVE)
                first = -first;

            ret[firstUse] = Long.valueOf(first);
            for (int i = firstUse + 1; i < BTree.DEFAULT_SIZE; i++) {
                //ret[i] = Serialization.readObject(ois);
                long v = LongPacker.unpackLong(ois);
                if (v == 0) continue; //null
                v = v + first;
                ret[i] = Long.valueOf(v);
                first = v;
            }
            return (K[]) ret;
        } else if (type == ALL_STRINGS) {
            byte[] previous = null;
            for (int i = firstUse; i < BTree.DEFAULT_SIZE; i++) {
                byte[] b = leadingValuePackRead(ois, previous, 0);
                if (b == null) continue;
                ret[i] = new String(b);
                previous = b;
            }
            return (K[]) ret;

        } else if (type == ALL_OTHER) {

            //TODO why this block is here?
            if (_btree.keySerializer == null || _btree.keySerializer == _btree.getRecordManager().defaultSerializer()) {
                for (int i = firstUse; i < BTree.DEFAULT_SIZE; i++) {
                    ret[i] = _btree.getRecordManager().defaultSerializer().deserialize(ois);
                }
                return (K[]) ret;
            }


            Serializer ser = _btree.keySerializer != null ? _btree.keySerializer : _btree.getRecordManager().defaultSerializer();
            DataInputOutput in2 = null;
            byte[] previous = null;
            for (int i = firstUse; i < BTree.DEFAULT_SIZE; i++) {
                byte[] b = leadingValuePackRead(ois, previous, 0);
                if (b == null) continue;
                if (in2 == null) {
                    in2 = new DataInputOutput();
                }
                in2.reset(b);
                ret[i] = ser.deserialize(in2);
                previous = b;
            }
            return (K[]) ret;

        } else {
            throw new InternalError("unknown BTreeNode header type: " + type);
        }

    }


    @SuppressWarnings("unchecked")
    private void writeKeys(DataOutput oos, K[] keys, final int firstUse) throws IOException {
        if (keys.length != BTree.DEFAULT_SIZE)
            throw new IllegalArgumentException("wrong keys size");

        //check if all items on key are null
        boolean allNull = true;
        for (int i = firstUse; i < BTree.DEFAULT_SIZE; i++) {
            if (keys[i] != null) {
                allNull = false;
                break;
            }
        }
        if (allNull) {
            oos.write(ALL_NULL);
            return;
        }

        /**
         * Special compression to compress Long and Integer
         */
        if ((_btree._comparator == Utils.COMPARABLE_COMPARATOR || _btree._comparator == null) &&
                (_btree.keySerializer == null || _btree.keySerializer == _btree.getRecordManager().defaultSerializer())) {
            boolean allInteger = true;
            for (int i = firstUse; i < BTree.DEFAULT_SIZE; i++) {
                if (keys[i] != null && keys[i].getClass() != Integer.class) {
                    allInteger = false;
                    break;
                }
            }
            boolean allLong = true;
            for (int i = firstUse; i < BTree.DEFAULT_SIZE; i++) {
                if (keys[i] != null && (keys[i].getClass() != Long.class ||
                        //special case to exclude Long.MIN_VALUE from conversion, causes problems to LongPacker
                        ((Long) keys[i]).longValue() == Long.MIN_VALUE)
                        ) {
                    allLong = false;
                    break;
                }
            }

            if (allLong) {
                //check that diff between MIN and MAX fits into PACKED_LONG
                long max = Long.MIN_VALUE;
                long min = Long.MAX_VALUE;
                for (int i = firstUse; i < BTree.DEFAULT_SIZE; i++) {
                    if (keys[i] == null) continue;
                    long v = (Long) keys[i];
                    if (v > max) max = v;
                    if (v < min) min = v;
                }
                //now convert to Double to prevent overflow errors
                double max2 = max;
                double min2 = min;
                double maxDiff = Long.MAX_VALUE;
                if (max2 - min2 > maxDiff / 2) // divide by two just to by sure
                    allLong = false;

            }

            if (allLong && allInteger)
                throw new InternalError();

            if (allLong || allInteger) {
                long first = ((Number) keys[firstUse]).longValue();
                //write header
                if (allInteger) {
                    if (first > 0) oos.write(ALL_INTEGERS);
                    else oos.write(ALL_INTEGERS_NEGATIVE);
                } else if (allLong) {
                    if (first > 0) oos.write(ALL_LONGS);
                    else oos.write(ALL_LONGS_NEGATIVE);
                } else {
                    throw new InternalError();
                }

                //write first
                LongPacker.packLong(oos, Math.abs(first));
                //write others
                for (int i = firstUse + 1; i < BTree.DEFAULT_SIZE; i++) {
//					Serialization.writeObject(oos, keys[i]);
                    if (keys[i] == null)
                        LongPacker.packLong(oos, 0);
                    else {
                        long v = ((Number) keys[i]).longValue();
                        if (v <= first) throw new InternalError("not ordered");
                        LongPacker.packLong(oos, v - first);
                        first = v;
                    }
                }
                return;
            } else {
                //another special case for Strings
                boolean allString = true;
                for (int i = firstUse; i < BTree.DEFAULT_SIZE; i++) {
                    if (keys[i] != null && (keys[i].getClass() != String.class)
                            ) {
                        allString = false;
                        break;
                    }
                }
                if (allString) {
                    oos.write(ALL_STRINGS);
                    byte[] previous = null;
                    for (int i = firstUse; i < BTree.DEFAULT_SIZE; i++) {
                        if (keys[i] == null) {
                            leadingValuePackWrite(oos, null, previous, 0);
                        } else {
                            byte[] b = ((String) keys[i]).getBytes();
                            leadingValuePackWrite(oos, b, previous, 0);
                            previous = b;
                        }
                    }
                    return;
                }
            }
        }

        /**
         * other case, serializer is provided or other stuff
         */
        oos.write(ALL_OTHER);
        if (_btree.keySerializer == null || _btree.keySerializer == _btree.getRecordManager().defaultSerializer()) {
            for (int i = firstUse; i < BTree.DEFAULT_SIZE; i++) {
                _btree.getRecordManager().defaultSerializer().serialize(oos, keys[i]);
            }
            return;
        }

        //custom serializer is provided, use it

        Serializer ser = _btree.keySerializer;
        byte[] previous = null;


        DataInputOutput out3 = new DataInputOutput();
        for (int i = firstUse; i < BTree.DEFAULT_SIZE; i++) {
            if (keys[i] == null) {
                leadingValuePackWrite(oos, null, previous, 0);
            } else {
                out3.reset();
                ser.serialize(out3, keys[i]);
                byte[] b = out3.toByteArray();
                leadingValuePackWrite(oos, b, previous, 0);
                previous = b;
            }
        }


    }

    public void defrag(DBStore r1, DBStore r2) throws IOException {
        if (_children != null)
            for (long child : _children) {
                if (child == 0) continue;
                byte[] data = r1.fetchRaw(child);
                r2.forceInsert(child, data);
                BTreeNode t = deserialize(new DataInputOutput(data));
                t._btree = _btree;
                t.defrag(r1, r2);
            }
    }


    /**
     * STATIC INNER CLASS
     * Result from insert() method call
     */
    static final class InsertResult<K, V> {

        /**
         * Overflow node.
         */
        BTreeNode<K, V> _overflow;

        /**
         * Existing value for the insertion key.
         */
        V _existing;

    }

    /**
     * STATIC INNER CLASS
     * Result from remove() method call
     */
    static final class RemoveResult<K, V> {

        /**
         * Set to true if underlying nodes underflowed
         */
        boolean _underflow;

        /**
         * Removed entry value
         */
        V _value;
    }


    /**
     * PRIVATE INNER CLASS
     * Browser to traverse leaf nodes.
     */
    static final class Browser<K, V>
            implements BTree.BTreeTupleBrowser<K, V> {

        /**
         * Current node.
         */
        private BTreeNode<K, V> _node;

        /**
         * Current index in the node.  The index positionned on the next
         * tuple to return.
         */
        private byte _index;


        private int expectedModCount;


        /**
         * Create a browser.
         *
         * @param node  Current node
         * @param index Position of the next tuple to return.
         */
        Browser(BTreeNode<K, V> node, byte index) {
            _node = node;
            _index = index;
            expectedModCount = node._btree.modCount;
        }

        public boolean getNext(BTree.BTreeTuple<K, V> tuple)
                throws IOException {
            if (expectedModCount != _node._btree.modCount)
                throw new ConcurrentModificationException();
            if (_node == null) {
                //last record in iterator was deleted, so iterator is at end of node
                return false;
            }

            if (_index < BTree.DEFAULT_SIZE) {
                if (_node._keys[_index] == null) {
                    // reached end of the tree.
                    return false;
                }
            } else if (_node._next != 0) {
                // move to next node
                _node = _node.loadNode(_node._next);
                _index = _node._first;
            }
            tuple.key = _node._keys[_index];
            if (_node._values[_index] instanceof BTreeLazyRecord)
                tuple.value = ((BTreeLazyRecord<V>) _node._values[_index]).get();
            else
                tuple.value = (V) _node._values[_index];
            _index++;
            return true;
        }

        public boolean getPrevious(BTree.BTreeTuple<K, V> tuple)
                throws IOException {
            if (expectedModCount != _node._btree.modCount)
                throw new ConcurrentModificationException();

            if (_node == null) {
                //deleted last record, but this situation is only supportedd on getNext
                throw new InternalError();
            }

            if (_index == _node._first) {

                if (_node._previous != 0) {
                    _node = _node.loadNode(_node._previous);
                    _index = BTree.DEFAULT_SIZE;
                } else {
                    // reached beginning of the tree
                    return false;
                }
            }
            _index--;
            tuple.key = _node._keys[_index];
            if (_node._values[_index] instanceof BTreeLazyRecord)
                tuple.value = ((BTreeLazyRecord<V>) _node._values[_index]).get();
            else
                tuple.value = (V) _node._values[_index];

            return true;

        }

        public void remove(K key) throws IOException {
            if (expectedModCount != _node._btree.modCount)
                throw new ConcurrentModificationException();

            _node._btree.remove(key);
            expectedModCount++;

            //An entry was removed and this may trigger tree rebalance,
            //This would change current node layout, so find our position again
            BTree.BTreeTupleBrowser b = _node._btree.browse(key,true);
            //browser is positioned just before value which was currently deleted, so find if we have new value
            if (b.getNext(new BTree.BTreeTuple(null, null))) {
                //next value value exists, copy its state
                Browser b2 = (Browser) b;
                this._node = b2._node;
                this._index = b2._index;
            } else {
                this._node = null;
                this._index = -1;
            }


        }
    }

    /**
     * Used for debugging and testing only.  Recursively obtains the recids of
     * all child BTreeNodes and adds them to the 'out' list.
     *
     * @param out
     * @param height
     * @throws IOException
     */
    void dumpChildNodeRecIDs(List out, int height)
            throws IOException {
        height -= 1;
        if (height > 0) {
            for (byte i = _first; i < BTree.DEFAULT_SIZE; i++) {
                if (_children[i] == 0) continue;

                BTreeNode child = loadNode(_children[i]);
                out.add(new Long(child._recid));
                child.dumpChildNodeRecIDs(out, height);
            }
        }
    }


    /**
     * Read previously written data
     *
     * @author Kevin Day
     */
    static byte[] leadingValuePackRead(DataInput in, byte[] previous, int ignoreLeadingCount) throws IOException {
        int len = LongPacker.unpackInt(in) - 1;  // 0 indicates null
        if (len == -1)
            return null;

        int actualCommon = LongPacker.unpackInt(in);

        byte[] buf = new byte[len];

        if (previous == null) {
            actualCommon = 0;
        }


        if (actualCommon > 0) {
            in.readFully(buf, 0, ignoreLeadingCount);
            System.arraycopy(previous, ignoreLeadingCount, buf, ignoreLeadingCount, actualCommon - ignoreLeadingCount);
        }
        in.readFully(buf, actualCommon, len - actualCommon);
        return buf;
    }

    /**
     * This method is used for delta compression for keys.
     * Writes the contents of buf to the DataOutput out, with special encoding if
     * there are common leading bytes in the previous group stored by this compressor.
     *
     * @author Kevin Day
     */
    static void leadingValuePackWrite(DataOutput out, byte[] buf, byte[] previous, int ignoreLeadingCount) throws IOException {
        if (buf == null) {
            LongPacker.packInt(out, 0);
            return;
        }

        int actualCommon = ignoreLeadingCount;

        if (previous != null) {
            int maxCommon = buf.length > previous.length ? previous.length : buf.length;

            if (maxCommon > Short.MAX_VALUE) maxCommon = Short.MAX_VALUE;

            for (; actualCommon < maxCommon; actualCommon++) {
                if (buf[actualCommon] != previous[actualCommon])
                    break;
            }
        }


        // there are enough common bytes to justify compression
        LongPacker.packInt(out, buf.length + 1);// store as +1, 0 indicates null
        LongPacker.packInt(out, actualCommon);
        out.write(buf, 0, ignoreLeadingCount);
        out.write(buf, actualCommon, buf.length - actualCommon);

    }


    BTreeNode<K, V> loadLastChildNode() throws IOException {
        return loadNode(_children[BTree.DEFAULT_SIZE - 1]);
    }


}
