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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * An abstract class implementing most of DB.
 * It also has some JDBM package protected stuff (getNamedRecord)
 */
abstract class DBAbstract implements DB {


    /**
     * Reserved slot for name directory recid.
     */
    static final byte NAME_DIRECTORY_ROOT = 0;


    /**
     * Reserved slot for version number
     */
    static final byte STORE_VERSION_NUMBER_ROOT = 1;

    /**
     * Reserved slot for recid where Serial class info is stored
     *
     * NOTE when introducing more roots, do not forget to update defrag
     */
    static final byte SERIAL_CLASS_INFO_RECID_ROOT = 2;

    /** to prevent double instances of the same collection, we use weak value map
     *
     * //TODO what to do when there is rollback?
     * //TODO clear on close
     */
    final private Map<String,WeakReference<Object>> collections = new HashMap<String,WeakReference<Object>>();



    /**
      * Inserts a new record using a custom serializer.
      *
      * @param obj        the object for the new record.
      * @param serializer a custom serializer
      * @return the rowid for the new record.
      * @throws java.io.IOException when one of the underlying I/O operations fails.
      */
    abstract <A> long insert(A obj, Serializer<A> serializer,boolean disableCache) throws IOException;

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
        return insert(obj, defaultSerializer(),false);
    }


    public void update(long recid, Object obj) throws IOException {
        update(recid, obj, defaultSerializer());
    }


    synchronized public <A> A fetch(long recid) throws IOException {
        return (A) fetch(recid, defaultSerializer());
    }

    synchronized public <K, V> ConcurrentMap<K, V> getHashMap(String name) {
        Object o = getCollectionInstance(name);
        if(o!=null)
            return (ConcurrentMap<K, V>) o;

        try {
            long recid = getNamedObject(name);
            if(recid == 0) return null;

            HTree tree = fetch(recid);
            tree.setPersistenceContext(this);
            if(!tree.hasValues()){
                throw new ClassCastException("HashSet is not HashMap");
            }
            collections.put(name,new WeakReference<Object>(tree));
            return tree;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    synchronized public <K, V> ConcurrentMap<K, V> createHashMap(String name) {
        return createHashMap(name, null, null);
    }


    public synchronized <K, V> ConcurrentMap<K, V> createHashMap(String name, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        try {
            assertNameNotExist(name);

            HTree<K, V> tree = new HTree(this, keySerializer, valueSerializer,true);
            long recid = insert(tree);
            setNamedObject(name, recid);
            collections.put(name,new WeakReference<Object>(tree));
            return tree;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public synchronized <K> Set<K> getHashSet(String name) {
        Object o = getCollectionInstance(name);
        if(o!=null)
            return (Set<K>) o;

        try {
            long recid = getNamedObject(name);
            if(recid == 0) return null;

            HTree tree = fetch(recid);
            tree.setPersistenceContext(this);
            if(tree.hasValues()){
                throw new ClassCastException("HashMap is not HashSet");
            }
            Set<K> ret  =  new HTreeSet(tree);
            collections.put(name,new WeakReference<Object>(ret));
            return ret;
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

            Set<K> ret =  new HTreeSet<K>(tree);
            collections.put(name,new WeakReference<Object>(ret));
            return ret;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    synchronized public <K, V> ConcurrentNavigableMap<K, V> getTreeMap(String name) {
        Object o = getCollectionInstance(name);
        if(o!=null)
            return (ConcurrentNavigableMap<K, V> ) o;

        try {
            long recid = getNamedObject(name);
            if(recid == 0) return null;

            BTree t =  BTree.<K, V>load(this, recid);
            if(!t.hasValues())
                throw new ClassCastException("TreeSet is not TreeMap");
            ConcurrentNavigableMap<K,V> ret = new BTreeMap<K, V>(t,false); //TODO put readonly flag here
            collections.put(name,new WeakReference<Object>(ret));
            return ret;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    synchronized public <K extends Comparable, V> ConcurrentNavigableMap<K, V> createTreeMap(String name) {
        return createTreeMap(name, null, null, null);
    }


    public synchronized <K, V> ConcurrentNavigableMap<K, V> createTreeMap(String name,
                                                             Comparator<K> keyComparator,
                                                             Serializer<K> keySerializer,
                                                             Serializer<V> valueSerializer) {
        try {
            assertNameNotExist(name);
            BTree<K, V> tree = BTree.createInstance(this, keyComparator, keySerializer, valueSerializer,true);
            setNamedObject(name, tree.getRecid());
            ConcurrentNavigableMap<K,V> ret = new BTreeMap<K, V>(tree,false); //TODO put readonly flag here
            collections.put(name,new WeakReference<Object>(ret));
            return ret;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }


    public synchronized <K> NavigableSet<K> getTreeSet(String name) {
        Object o = getCollectionInstance(name);
        if(o!=null)
            return (NavigableSet<K> ) o;

        try {
            long recid = getNamedObject(name);
            if(recid == 0) return null;

            BTree t =  BTree.<K, Object>load(this, recid);
            if(t.hasValues())
                throw new ClassCastException("TreeMap is not TreeSet");
            BTreeSet<K> ret =  new BTreeSet<K>(new BTreeMap(t,false));
            collections.put(name,new WeakReference<Object>(ret));
            return ret;

        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public synchronized <K> NavigableSet<K> createTreeSet(String name) {
        return createTreeSet(name, null, null);
    }


    public synchronized <K> NavigableSet<K> createTreeSet(String name, Comparator<K> keyComparator, Serializer<K> keySerializer) {
        try {
            assertNameNotExist(name);
            BTree<K, Object> tree = BTree.createInstance(this, keyComparator, keySerializer, null,false);
            setNamedObject(name, tree.getRecid());
            BTreeSet<K> ret =  new BTreeSet<K>(new BTreeMap(tree,false));
            collections.put(name,new WeakReference<Object>(ret));
            return ret;

        } catch (IOException e) {
            throw new IOError(e);
        }

    }


    synchronized public <K> List<K> createLinkedList(String name) {
        return createLinkedList(name, null);
    }

    synchronized public <K> List<K> createLinkedList(String name, Serializer<K> serializer) {
        try {
            assertNameNotExist(name);

            //allocate record and overwrite it

            LinkedList2<K> list = new LinkedList2<K>(this, serializer);
            long recid = insert(list);
            setNamedObject(name, recid);

            collections.put(name,new WeakReference<Object>(list));

            return list;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    synchronized public <K> List<K> getLinkedList(String name) {
        Object o = getCollectionInstance(name);
        if(o!=null)
            return (List<K> ) o;

        try {
            long recid = getNamedObject(name);
            if(recid == 0) return null;
            LinkedList2<K> list = (LinkedList2<K>) fetch(recid);
            list.setPersistenceContext(this);
            collections.put(name,new WeakReference<Object>(list));
            return list;
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
    
    private synchronized  Object getCollectionInstance(String name){
        WeakReference ref = collections.get(name);
        if(ref==null)return null;
        Object o = ref.get();
        if(o != null) return o;
        //already GCed
        collections.remove(name);
        return null;
    }


    private void assertNameNotExist(String name) throws IOException {
        if (getNamedObject(name) != 0)
            throw new IllegalArgumentException("Object with name '" + name + "' already exists");
    }



    /**
     * Obtain the record id of a named object. Returns 0 if named object
     * doesn't exist.
     * Named objects are used to store Map views and other well known objects.
     */
    protected long getNamedObject(String name) throws IOException{
        long nameDirectory_recid = getRoot(NAME_DIRECTORY_ROOT);
        if(nameDirectory_recid == 0){
            return 0;
        }
        HTree<String,Long> m = fetch(nameDirectory_recid);
        Long res = m.get(name);
        if(res == null)
            return 0;
        return res;
    }


    /**
     * Set the record id of a named object.
     * Named objects are used to store Map views and other well known objects.
     */
    protected void setNamedObject(String name, long recid) throws IOException{
        long nameDirectory_recid = getRoot(NAME_DIRECTORY_ROOT);
        HTree<String,Long> m = null;
        if(nameDirectory_recid == 0){
            //does not exists, create it
            m = new HTree<String, Long>(this,null,null,true);
            nameDirectory_recid = insert(m);
            setRoot(NAME_DIRECTORY_ROOT,nameDirectory_recid);
        }else{
            //fetch it
            m = fetch(nameDirectory_recid);
        }
        m.put(name,recid);
    }




    public Map<String,Object> getCollections(){
        try{
            Map<String,Object> ret = new LinkedHashMap<String, Object>();
            long nameDirectory_recid = getRoot(NAME_DIRECTORY_ROOT);
            if(nameDirectory_recid==0)
                return ret;
            HTree<String,Long> m = fetch(nameDirectory_recid);

            for(Map.Entry<String,Long> e:m.entrySet()){
                Object o = fetch(e.getValue());
                if(o instanceof BTree){
                    if(((BTree) o).hasValues)
                        o = getTreeMap(e.getKey());
                    else
                        o = getTreeSet(e.getKey());
                }
                else if( o instanceof  HTree){
                    if(((HTree) o).hasValues)
                        o = getHashMap(e.getKey());
                    else
                        o = getHashSet(e.getKey());
                }

                ret.put(e.getKey(), o);
            }
            return Collections.unmodifiableMap(ret);
        }catch(IOException e){
            throw new IOError(e);
        }

    }


    public void deleteCollection(String name){
        try{
            long nameDirectory_recid = getRoot(NAME_DIRECTORY_ROOT);
            if(nameDirectory_recid==0)
                throw new IOException("Collection not found");
            HTree<String,Long> dir = fetch(nameDirectory_recid);

            Long recid = dir.get(name);
            if(recid == null) throw new IOException("Collection not found");

            Object o = fetch(recid);
            //we can not use O instance since it is not correctly initialized
            if(o instanceof LinkedList2){
                LinkedList2 l = (LinkedList2) o;
                l.clear();
                delete(l.rootRecid);
            }else if(o instanceof BTree){
                ((BTree) o).clear();
            } else if( o instanceof  HTree){
                HTree t = (HTree) o;
                t.clear();
                HTreeDirectory n = (HTreeDirectory) fetch(t.rootRecid,t.SERIALIZER);
                n.deleteAllChildren();
                delete(t.rootRecid);
            }else{
                throw new InternalError("unknown collection type: "+(o==null?null:o.getClass()));
            }
            delete(recid);
            collections.remove(name);


            dir.remove(name);

        }catch(IOException e){
            throw new IOError(e);
        }

    }


    /** we need to set reference to this DB instance, so serializer needs to be here*/
    final Serializer<Serialization> defaultSerializationSerializer = new Serializer<Serialization>(){

        public void serialize(DataOutput out, Serialization obj) throws IOException {
            LongPacker.packLong(out,obj.serialClassInfoRecid);
            SerialClassInfo.serializer.serialize(out,obj.registered);
        }

        public Serialization deserialize(DataInput in) throws IOException, ClassNotFoundException {
            final long recid = LongPacker.unpackLong(in);
            final ArrayList<SerialClassInfo.ClassInfo> classes = SerialClassInfo.serializer.deserialize(in);
            return new Serialization(DBAbstract.this,recid,classes);
        }
    };

    public synchronized Serializer defaultSerializer() {

        try{
            long serialClassInfoRecid = getRoot(SERIAL_CLASS_INFO_RECID_ROOT);
            if (serialClassInfoRecid == 0) {                
                //allocate new recid 
                serialClassInfoRecid = insert(null,Utils.NULL_SERIALIZER,false);
                //and insert new serializer
                Serialization ser = new Serialization(this,serialClassInfoRecid,new ArrayList<SerialClassInfo.ClassInfo>());

                update(serialClassInfoRecid,ser, defaultSerializationSerializer);
                setRoot(SERIAL_CLASS_INFO_RECID_ROOT, serialClassInfoRecid);
                return ser;
            }else{
                return fetch(serialClassInfoRecid,defaultSerializationSerializer);
            }

        } catch (IOException e) {
            throw new IOError(e);
        }

    }

    
    final protected void checkNotClosed(){
        if(isClosed()) throw new IllegalStateException("db was closed");
    }

    protected abstract void setRoot(byte root, long recid);
    protected abstract long getRoot(byte root);


    public long collectionSize(Object collection){
        if(collection instanceof BTreeMap){
            BTreeMap t = (BTreeMap) collection;
            if(t.fromKey!=null|| t.toKey!=null) throw new IllegalArgumentException("collectionSize does not work on BTree submap");
            return t.tree._entries;
        }else if(collection instanceof  HTree){
          return ((HTree)collection).getRoot().size;
        }else if(collection instanceof  HTreeSet){
            return collectionSize(((HTreeSet) collection).map);
        }else if(collection instanceof  BTreeSet){
            return collectionSize(((BTreeSet) collection).map);
        }else if(collection instanceof LinkedList2){
            return ((LinkedList2)collection).getRoot().size;
        }else{
            throw new IllegalArgumentException("Not JDBM collection");
        }
    }


    void addShutdownHook(){
        if(shutdownCloseThread!=null){
            shutdownCloseThread = new ShutdownCloseThread();
            Runtime.getRuntime().addShutdownHook(shutdownCloseThread);
        }
    }

    public void close(){
        if(shutdownCloseThread!=null){
            Runtime.getRuntime().removeShutdownHook(shutdownCloseThread);
            shutdownCloseThread.dbToClose = null;
            shutdownCloseThread = null;
        }
    }


    ShutdownCloseThread shutdownCloseThread = null;

    private static class ShutdownCloseThread extends Thread{

        DBAbstract dbToClose = null;

        ShutdownCloseThread(){
            super("JDBM shutdown");
        }

        public void run(){
            if(dbToClose!=null && !dbToClose.isClosed()){
                dbToClose.shutdownCloseThread = null;
                dbToClose.close();
            }
        }

    }
}
