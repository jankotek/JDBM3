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

import java.io.IOError;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;


/**
 * Wrapper for <code>BTree</code> which implements <code>ConcurrentNavigableMap</code> interface
 *
 * @param <K> key type
 * @param <V> value type
 *
 * @author Jan Kotek
 */
class BTreeMap<K, V> extends AbstractMap<K, V> implements ConcurrentNavigableMap<K, V> {

    protected BTree<K, V> tree;

    protected final K fromKey;

    protected final K toKey;

    protected final boolean readonly;

    protected NavigableSet<K> keySet2;
    private final boolean toInclusive;
    private final boolean fromInclusive;

    public BTreeMap(BTree<K, V> tree, boolean readonly) {
        this(tree, readonly, null, false, null, false);
    }

    protected BTreeMap(BTree<K, V> tree, boolean readonly, K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        this.tree = tree;
        this.fromKey = fromKey;
        this.fromInclusive = fromInclusive;
        this.toKey = toKey;
        this.toInclusive = toInclusive;
        this.readonly = readonly;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return _entrySet;
    }



    private final Set<java.util.Map.Entry<K, V>> _entrySet = new AbstractSet<Entry<K, V>>() {

        protected Entry<K, V> newEntry(K k, V v) {
            return new SimpleEntry<K, V>(k, v) {
                private static final long serialVersionUID = 978651696969194154L;

                public V setValue(V arg0) {
                    BTreeMap.this.put(getKey(), arg0);
                    return super.setValue(arg0);
                }

            };
        }

        public boolean add(java.util.Map.Entry<K, V> e) {
            if (readonly)
                throw new UnsupportedOperationException("readonly");

            try {
                if (e.getKey() == null)
                    throw new NullPointerException("Can not add null key");
                if (!inBounds(e.getKey()))
                    throw new IllegalArgumentException("key outside of bounds");
                return tree.insert(e.getKey(), e.getValue(), true) == null;
            } catch (IOException e1) {
                throw new IOError(e1);
            }
        }

        @SuppressWarnings("unchecked")
        public boolean contains(Object o) {

            if (o instanceof Entry) {
                Entry<K, V> e = (java.util.Map.Entry<K, V>) o;
                try {
                    if (!inBounds(e.getKey()))
                        return false;
                    if (e.getKey() != null && tree.get(e.getKey()) != null)
                        return true;
                } catch (IOException e1) {
                    throw new IOError(e1);
                }
            }
            return false;
        }


        public Iterator<java.util.Map.Entry<K, V>> iterator() {
            try {
                final BTree.BTreeTupleBrowser<K, V> br = fromKey == null ?
                        tree.browse() : tree.browse(fromKey, fromInclusive);
                return new Iterator<Entry<K, V>>() {

                    private Entry<K, V> next;
                    private K lastKey;

                    void ensureNext() {
                        try {
                            BTree.BTreeTuple<K, V> t = new BTree.BTreeTuple<K, V>();
                            if (br.getNext(t) && inBounds(t.key))
                                next = newEntry(t.key, t.value);
                            else
                                next = null;
                        } catch (IOException e1) {
                            throw new IOError(e1);
                        }
                    }

                    {
                        ensureNext();
                    }


                    public boolean hasNext() {
                        return next != null;
                    }

                    public java.util.Map.Entry<K, V> next() {
                        if (next == null)
                            throw new NoSuchElementException();
                        Entry<K, V> ret = next;
                        lastKey = ret.getKey();
                        //move to next position
                        ensureNext();
                        return ret;
                    }

                    public void remove() {
                        if (readonly)
                            throw new UnsupportedOperationException("readonly");

                        if (lastKey == null)
                            throw new IllegalStateException();
                        try {
                            br.remove(lastKey);
                            lastKey = null;
                        } catch (IOException e1) {
                            throw new IOError(e1);
                        }

                    }
                };

            } catch (IOException e) {
                throw new IOError(e);
            }

        }

        @SuppressWarnings("unchecked")
        public boolean remove(Object o) {
            if (readonly)
                throw new UnsupportedOperationException("readonly");

            if (o instanceof Entry) {
                Entry<K, V> e = (java.util.Map.Entry<K, V>) o;
                try {
                    //check for nulls
                    if (e.getKey() == null || e.getValue() == null)
                        return false;
                    if (!inBounds(e.getKey()))
                        throw new IllegalArgumentException("out of bounds");
                    //get old value, must be same as item in entry
                    V v = get(e.getKey());
                    if (v == null || !e.getValue().equals(v))
                        return false;
                    V v2 = tree.remove(e.getKey());
                    return v2 != null;
                } catch (IOException e1) {
                    throw new IOError(e1);
                }
            }
            return false;

        }

        public int size() {
            return BTreeMap.this.size();
        }

        public void clear(){
            if(fromKey!=null || toKey!=null)
                super.clear();
            else
                try {
                    tree.clear();
                } catch (IOException e) {
                    throw new IOError(e);
                }
        }

    };


    public boolean inBounds(K e) {
        if(fromKey == null && toKey == null)
            return true;

        Comparator comp = comparator();
        if (comp == null) comp = Utils.COMPARABLE_COMPARATOR;
        
        if(fromKey!=null){
            final int compare = comp.compare(e, fromKey);
            if(compare<0) return false;
            if(!fromInclusive && compare == 0) return false;
        }
        if(toKey!=null){
            final int compare = comp.compare(e, toKey);
            if(compare>0)return false;
            if(!toInclusive && compare == 0) return false;
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get(Object key) {
        try {
            if (key == null)
                return null;
            if (!inBounds((K) key))
                return null;
            return tree.get((K) key);
        } catch (ClassCastException e) {
            return null;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public V remove(Object key) {
        if (readonly)
            throw new UnsupportedOperationException("readonly");

        try {
            if (key == null || tree.get((K) key) == null)
                return null;
            if (!inBounds((K) key))
                throw new IllegalArgumentException("out of bounds");

            return tree.remove((K) key);
        } catch (ClassCastException e) {
            return null;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public V put(K key, V value) {
        if (readonly)
            throw new UnsupportedOperationException("readonly");

        try {
            if (key == null || value == null)
                throw new NullPointerException("Null key or value");
            if (!inBounds(key))
                throw new IllegalArgumentException("out of bounds");
            return tree.insert(key, value, true);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public void clear(){
        entrySet().clear();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean containsKey(Object key) {
        if (key == null)
            return false;
        try {
            if (!inBounds((K) key))
                return false;
            V v = tree.get((K) key);
            return v != null;
        } catch (IOException e) {
            throw new IOError(e);
        } catch (ClassCastException e) {
            return false;
        }
    }

    public Comparator<? super K> comparator() {
        return tree._comparator;
    }

    public K firstKey() {
        if (isEmpty())
            return null;
        try {

            BTree.BTreeTupleBrowser<K, V> b = fromKey == null ? tree.browse() : tree.browse(fromKey,fromInclusive);
            BTree.BTreeTuple<K, V> t = new BTree.BTreeTuple<K, V>();
            b.getNext(t);
            return t.key;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public K lastKey() {
        if (isEmpty())
            return null;
        try {
            BTree.BTreeTupleBrowser<K, V> b = toKey == null ? tree.browse(null,true) : tree.browse(toKey,false);
            BTree.BTreeTuple<K, V> t = new BTree.BTreeTuple<K, V>();            
            b.getPrevious(t);            
            if(!toInclusive && toKey!=null){
                //make sure we wont return last key
                Comparator c = comparator();
                if(c==null) c=Utils.COMPARABLE_COMPARATOR;
                if(c.compare(t.key,toKey)==0)
                    b.getPrevious(t);
            }
            return t.key;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public ConcurrentNavigableMap<K, V> headMap(K toKey2, boolean inclusive) {
        K toKey3 = Utils.min(this.toKey,toKey2,comparator());
        boolean inclusive2 = toKey3 == toKey? toInclusive : inclusive;
        return new BTreeMap<K, V>(tree, readonly, this.fromKey, this.fromInclusive, toKey3, inclusive2);
    }


    public ConcurrentNavigableMap<K, V> headMap(K toKey) {
        return headMap(toKey,false);
    }


    public Entry<K, V> lowerEntry(K key) {
        K k = lowerKey(key);
        return k==null? null : new SimpleEntry<K, V>(k,get(k));
    }

    public K lowerKey(K key) {
        if (isEmpty())
            return null;
        K key2 = Utils.min(key,toKey,comparator());
        try {
            BTree.BTreeTupleBrowser<K, V> b  = tree.browse(key2,true) ;
            BTree.BTreeTuple<K, V> t = new BTree.BTreeTuple<K, V>();
            b.getPrevious(t);

            return t.key;

        } catch (IOException e) {
            throw new IOError(e);
        }

    }

    public Entry<K, V> floorEntry(K key) {
        K k = floorKey(key);
        return k==null? null : new SimpleEntry<K, V>(k,get(k));

    }

    public K floorKey(K key) {
        if (isEmpty())
            return null;

        K key2 = Utils.max(key,fromKey,comparator());
        try {
            BTree.BTreeTupleBrowser<K, V> b  = tree.browse(key2,true) ;
            BTree.BTreeTuple<K, V> t = new BTree.BTreeTuple<K, V>();
            b.getNext(t);
            Comparator comp = comparator();
            if (comp == null) comp = Utils.COMPARABLE_COMPARATOR;
            if(comp.compare(t.key,key2) == 0)
                return t.key;

            b.getPrevious(t);
            b.getPrevious(t);
            return t.key;

        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public Entry<K, V> ceilingEntry(K key) {
        K k = ceilingKey(key);
        return k==null? null : new SimpleEntry<K, V>(k,get(k));
    }

    public K ceilingKey(K key) {
        if (isEmpty())
            return null;
        K key2 = Utils.min(key,toKey,comparator());

        try {
            BTree.BTreeTupleBrowser<K, V> b  = tree.browse(key2,true) ;
            BTree.BTreeTuple<K, V> t = new BTree.BTreeTuple<K, V>();
            b.getNext(t);
            return t.key;

        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public Entry<K, V> higherEntry(K key) {
        K k = higherKey(key);
        return k==null? null : new SimpleEntry<K, V>(k,get(k));
    }

    public K higherKey(K key) {
        if (isEmpty())
            return null;

        K key2 = Utils.max(key,fromKey,comparator());

        try {
            BTree.BTreeTupleBrowser<K, V> b  = tree.browse(key2,false) ;
            BTree.BTreeTuple<K, V> t = new BTree.BTreeTuple<K, V>();
            b.getNext(t);
            return t.key;

        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public Entry<K, V> firstEntry() {
        K k = firstKey();
        return k==null? null : new SimpleEntry<K, V>(k,get(k));
    }

    public Entry<K, V> lastEntry() {
        K k = lastKey();
        return k==null? null : new SimpleEntry<K, V>(k,get(k));
    }

    public Entry<K, V> pollFirstEntry() {
        Entry<K,V> first = firstEntry();
        if(first!=null)
            remove(first.getKey());
        return first;
    }

    public Entry<K, V> pollLastEntry() {
        Entry<K,V> last = lastEntry();
        if(last!=null)
            remove(last.getKey());
        return last;
    }

    public ConcurrentNavigableMap<K, V> descendingMap() {
        throw new  UnsupportedOperationException("not implemented yet");
        //TODO implement descending (reverse order) map
    }


    public NavigableSet<K> keySet() {
        return navigableKeySet();
    }

    public NavigableSet<K> navigableKeySet() {
        if(keySet2 == null)
            keySet2 = new BTreeSet<K>((BTreeMap<K,Object>) this);
        return keySet2;
    }

    public NavigableSet<K> descendingKeySet() {
        return descendingMap().navigableKeySet();
    }



    public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
        return tailMap(fromKey,true);
    }


    public ConcurrentNavigableMap<K, V> tailMap(K fromKey2, boolean inclusive) {
        K fromKey3 = Utils.max(this.fromKey,fromKey2,comparator());
        boolean inclusive2 = fromKey3 == toKey? toInclusive : inclusive;

        return new BTreeMap<K, V>(tree, readonly, fromKey3, inclusive2, toKey, toInclusive);
    }

    public ConcurrentNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        Comparator comp = comparator();
        if (comp == null) comp = Utils.COMPARABLE_COMPARATOR;
        if (comp.compare(fromKey, toKey) > 0)
            throw new IllegalArgumentException("fromKey is bigger then toKey");
        return new BTreeMap<K, V>(tree, readonly, fromKey, fromInclusive, toKey, toInclusive);
    }

    public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
        return subMap(fromKey,true,toKey,false);
    }


    public BTree<K, V> getTree() {
        return tree;
    }


    public void addRecordListener(RecordListener<K, V> listener) {
        tree.addRecordListener(listener);
    }

    public DBAbstract getRecordManager() {
        return tree.getRecordManager();
    }

    public void removeRecordListener(RecordListener<K, V> listener) {
        tree.removeRecordListener(listener);
    }


    public int size() {
        if (fromKey == null && toKey == null)
            return (int) tree._entries; //use fast counter on tree if Map has no bounds
        else {
            //had to count items in iterator
            Iterator iter = keySet().iterator();
            int counter = 0;
            while (iter.hasNext()) {
                iter.next();
                counter++;
            }
            return counter;
        }

    }


    public V putIfAbsent(K key, V value) {
        tree.lock.writeLock().lock();
        try{
            if (!containsKey(key))
                 return put(key, value);
            else
                 return get(key);
        }finally {
            tree.lock.writeLock().unlock();
        }
    }

    public boolean remove(Object key, Object value) {
        tree.lock.writeLock().lock();
        try{
            if (containsKey(key) && get(key).equals(value)) {
                remove(key);
                return true;
            } else return false;
        }finally {
            tree.lock.writeLock().unlock();
        }


    }

    public boolean replace(K key, V oldValue, V newValue) {
        tree.lock.writeLock().lock();
        try{
            if (containsKey(key) && get(key).equals(oldValue)) {
                put(key, newValue);
                return true;
            } else return false;
        }finally {
            tree.lock.writeLock().unlock();
        }

    }

    public V replace(K key, V value) {
        tree.lock.writeLock().lock();
        try{
            if (containsKey(key)) {
                return put(key, value);
             } else return null;
        }finally {
            tree.lock.writeLock().unlock();
        }
    }
}
