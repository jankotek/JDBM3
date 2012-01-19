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

import java.io.IOError;
import java.io.IOException;
import java.util.*;

/**
 * An abstract class implementing most of DB.
 * It also has some JDBM package protected stuff (getNamedRecord)
 */
abstract class DBAbstract implements DB {


    /**
     * Inserts a new record using a custom serializer.
     *
     * @param obj        the object for the new record.
     * @param serializer a custom serializer
     * @return the rowid for the new record.
     * @throws java.io.IOException when one of the underlying I/O operations fails.
     */
    abstract <A> long insert(A obj, Serializer<A> serializer) throws IOException;

    /**
     * Deletes a record.
     *
     * @param recid the rowid for the record that should be deleted.
     * @throws java.io.IOException when one of the underlying I/O operations fails.
     */
    abstract void delete(long recid) throws IOException;


    /**
     * Updates a record using a custom serializer.
     * If given recid does not exist, IOException will be thrown before/during commit (cache).
     *
     * @param recid      the recid for the record that is to be updated.
     * @param obj        the new object for the record.
     * @param serializer a custom serializer
     * @throws java.io.IOException when one of the underlying I/O operations fails
     */
    abstract <A> void update(long recid, A obj, Serializer<A> serializer)
            throws IOException;


    /**
     * Fetches a record using a custom serializer.
     *
     * @param recid      the recid for the record that must be fetched.
     * @param serializer a custom serializer
     * @return the object contained in the record, null if given recid does not exist
     * @throws java.io.IOException when one of the underlying I/O operations fails.
     */
    abstract <A> A fetch(long recid, Serializer<A> serializer)
            throws IOException;

    /**
     * Fetches a record using a custom serializer and optionaly disabled cache
     *
     * @param recid        the recid for the record that must be fetched.
     * @param serializer   a custom serializer
     * @param disableCache true to disable any caching mechanism
     * @return the object contained in the record, null if given recid does not exist
     * @throws java.io.IOException when one of the underlying I/O operations fails.
     */
    abstract <A> A fetch(long recid, Serializer<A> serializer, boolean disableCache)
            throws IOException;


    public long insert(Object obj) throws IOException {
        return insert(obj, defaultSerializer());
    }


    public void update(long recid, Object obj) throws IOException {
        update(recid, obj, defaultSerializer());
    }


    public <A> A fetch(long recid) throws IOException {
        return (A) fetch(recid, defaultSerializer());
    }

    public <K, V> Map<K, V> getHashMap(String name) {
        try {
            long recid = assertNameExist(name);
            HTree tree = fetch(recid);
            tree.setPersistenceContext(this);
            if(!tree.hasValues()){
                throw new ClassCastException("HashSet is not HashMap");
            }
            return tree;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public <K, V> Map<K, V> createHashMap(String name) {
        return createHashMap(name, null, null);
    }


    public synchronized <K, V> Map<K, V> createHashMap(String name, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        try {
            assertNameNotExist(name);

            HTree<K, V> tree = new HTree(this, keySerializer, valueSerializer,true);
            long recid = insert(tree);
            setNamedObject(name, recid);

            return tree;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public synchronized <K> Set<K> getHashSet(String name) {
        try {
            long recid = assertNameExist(name);
            HTree tree = fetch(recid);
            tree.setPersistenceContext(this);
            if(tree.hasValues()){
                throw new ClassCastException("HashMap is not HashSet");
            }
            return  new HTreeSet(tree);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public synchronized <K> Set<K> createHashSet(String name) {
        return createHashSet(name, null);
    }

    public synchronized <K> Set<K> createHashSet(String name, Serializer<K> keySerializer) {
        try {
            assertNameNotExist(name);

            HTree<K, Object> tree = new HTree(this, keySerializer, null,false);
            long recid = insert(tree);
            setNamedObject(name, recid);

            return new HTreeSet<K>(tree);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public <K, V> SortedMap<K, V> getTreeMap(String name) {
        try {
            long recid = assertNameExist(name);
            BTree t =  BTree.<K, V>load(this, recid);
            if(!t.hasValues())
                throw new ClassCastException("TreeSet is not TreeMap");
            return t.asMap();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public <K extends Comparable, V> SortedMap<K, V> createTreeMap(String name) {
        return createTreeMap(name, null, null, null);
    }


    public synchronized <K, V> SortedMap<K, V> createTreeMap(String name,
                                                             Comparator<K> keyComparator,
                                                             Serializer<K> keySerializer,
                                                             Serializer<V> valueSerializer) {
        try {
            assertNameNotExist(name);
            BTree<K, V> tree = BTree.createInstance(this, keyComparator, keySerializer, valueSerializer,true);
            setNamedObject(name, tree.getRecid());
            return tree.asMap();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }


    public synchronized <K> SortedSet<K> getTreeSet(String name) {
        try {
            long recid = assertNameExist(name);
            BTree t =  BTree.<K, Object>load(this, recid);
            if(t.hasValues())
                throw new ClassCastException("TreeMap is not TreeSet");
            return new BTreeSet<K>(t.asMap());
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public synchronized <K> SortedSet<K> createTreeSet(String name) {
        return createTreeSet(name, null, null);
    }


    public synchronized <K> SortedSet<K> createTreeSet(String name, Comparator<K> keyComparator, Serializer<K> keySerializer) {
        try {
            assertNameNotExist(name);
            BTree<K, Object> tree = BTree.createInstance(this, keyComparator, keySerializer, null,false);
            setNamedObject(name, tree.getRecid());
            return new BTreeSet<K>(tree.asMap());
        } catch (IOException e) {
            throw new IOError(e);
        }

    }


    public <K> List<K> createLinkedList(String name) {
        return createLinkedList(name, null);
    }

    public <K> List<K> createLinkedList(String name, Serializer<K> serializer) {
        try {
            assertNameNotExist(name);

            //allocate record and overwrite it

            LinkedList<K> list = new LinkedList<K>(this, serializer);
            long recid = insert(list);
            setNamedObject(name, recid);


            return list;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public <K> List<K> getLinkedList(String name) {
        try {
            long recid = assertNameExist(name);

            LinkedList<K> list = (LinkedList<K>) fetch(recid);
            list.setPersistenceContext(this);
            return list;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }


    private void assertNameNotExist(String name) throws IOException {
        if (getNamedObject(name) != 0)
            throw new IllegalArgumentException("Object with name '" + name + "' already exists");
    }

    private long assertNameExist(String name) throws IOException {
        long recid = getNamedObject(name);
        if (recid == 0)
            throw new IllegalArgumentException("Object with name '" + name + "' does not exist");
        return recid;
    }


    /**
     * Obtain the record id of a named object. Returns 0 if named object
     * doesn't exist.
     * Named objects are used to store Map views and other well known objects.
     */
    protected abstract long getNamedObject(String name)
            throws IOException;


    /**
     * Set the record id of a named object.
     * Named objects are used to store Map views and other well known objects.
     */
    protected abstract void setNamedObject(String name, long recid)
            throws IOException;

    protected abstract Serializer defaultSerializer();

}
