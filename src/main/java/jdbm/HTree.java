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

package jdbm;

import java.io.*;
import java.util.*;

/**
 *  Persistent hashtable implementation for PageManager.
 *  Implemented as an H*Tree structure.
 *
 *  WARNING!  If this instance is used in a transactional context, it
 *            *must* be discarded after a rollback.
 *
 *  @author Alex Boisvert
 */
class HTree<K,V>  extends AbstractPrimaryMap<K,V> implements PrimaryHashMap<K,V>
{

    final Serializer SERIALIZER = new Serializer<Object>() {

        public Object deserialize(DataInput ds2) throws IOException {
            SerializerInput ds = (SerializerInput) ds2;
            try{
                int i = ds.read();
                if(i == Serialization.HTREE_BUCKET){ //is HashBucket?
                    HTreeBucket ret = new HTreeBucket(HTree.this);
                    ret.readExternal(ds);
                    if(ds.available()!=0 && ds.read()!=-1) // -1 is fix for compression, not sure what is happening
                        throw new InternalError("bytes left: "+ds.available());
                    return ret;
                }else if( i == Serialization.HTREE_DIRECTORY){
                    HTreeDirectory ret = new HTreeDirectory(HTree.this);
                    ret.readExternal(ds);
                    if(ds.available()!=0 && ds.read()!=-1) // -1 is fix for compression, not sure what is happening
                        throw new InternalError("bytes left: "+ds.available());
                    return ret;
                }else {
                    throw new InternalError("Wrong HTree header: "+i);
                }
            }catch(ClassNotFoundException e){
                throw new IOException(e);
            }

        }
        public void serialize(DataOutput out, Object obj) throws IOException {
            if(obj instanceof HTreeBucket){
                out.write(Serialization.HTREE_BUCKET);
                HTreeBucket b = (HTreeBucket) obj;
                b.writeExternal(out);
            }else{
                out.write(Serialization.HTREE_DIRECTORY);
                HTreeDirectory n = (HTreeDirectory) obj;
                n.writeExternal(out);
            }
        }
    };


    /**
     * Root hash directory.
     */
    private HTreeDirectory<K,V> _root;


    /**
     * Listeners which are notified about changes in records
     */
    protected List<RecordListener<K,V>> recordListeners = new ArrayList<RecordListener<K,V>>();

 /**
     * Serializer used to serialize index keys (optional)
     */
    protected Serializer<K> keySerializer;


    /**
     * Serializer used to serialize index values (optional)
     */
    protected Serializer<V> valueSerializer;
    protected boolean readonly = false;


    public Serializer<K> getKeySerializer() {
		return keySerializer;
	}

	public Serializer<V> getValueSerializer() {
		return valueSerializer;
	}


    HTree() {
    }


    /**
     * Create a persistent hashtable.
     *
     * @param recman Record manager used for persistence.
     */
    public HTree( RecordManager recman )
        throws IOException
    {
       this(recman, null, null);
    }

    /**
     * Create a persistent hashtable.
     *
     * @param recman Record manager used for persistence.
     */
    public HTree( RecordManager recman,
                                                   Serializer<K> keySerializer,
                                                   Serializer<V> valueSerializer )
        throws IOException
    {
        HTreeDirectory<K,V> root;
        long           recid;

        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        this._root = new HTreeDirectory<K,V>( this, (byte) 0 );
        recid = recman.insert( this._root, this.SERIALIZER );
        this._root.setPersistenceContext(recman, recid);

    }


    /**
     * Load a persistent hashtable
     *
     * @param recman RecordManager used to store the persistent hashtable
     * @param root_recid Record id of the root directory of the HTree
     */
    public HTree( RecordManager recman,
                                          long root_recid)
    throws IOException
    {
        this(recman, root_recid, null, null);
    }
    /**
     * Load a persistent hashtable
     *
     * @param recman RecordManager used to store the persistent hashtable
     * @param root_recid Record id of the root directory of the HTree
     */
    public HTree( RecordManager recman,
                                          long root_recid,
                                          Serializer<K> keySerializer,
                                          Serializer<V> valueSerializer )
        throws IOException
    {
        HTreeDirectory<K,V> root;

        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        this._root = (HTreeDirectory<K,V>) recman.fetch( root_recid, this.SERIALIZER  );
        this._root.setPersistenceContext( recman, root_recid );


    }

    transient long hashEqualsIdentityCounter=0;
    
    public synchronized V put(K key, V value) {
	if(readonly)
		throw new UnsupportedOperationException("readonly");

	try {
		if(key == null || value == null)
			throw new NullPointerException("Null key or value");
		V oldVal = get(key);
                _root.put(key, value);
                if(oldVal == null){
                    for(RecordListener<K,V> r : recordListeners)
                            r.recordInserted(key,value);
                }else{
                    for(RecordListener<K,V> r : recordListeners)
                            r.recordUpdated(key,oldVal,value);
                }

		return oldVal;
	} catch (IOException e) {
		throw new IOError(e);
	}
    }


    public synchronized V get(Object key)
    {
        try{
	    if(key == null)
		return null;
	    return _root.get((K) key);
	}catch (ClassCastException e){
	    return null;
	}catch (IOException e){
	    throw new IOError(e);
        }
    }


    public synchronized V remove(Object key) {
	if(readonly)
		throw new UnsupportedOperationException("readonly");

	try{
		if(key == null)
			return null;
		V oldVal = get((K) key);
		if(oldVal!=null){
                           V val = null;
                           if(!recordListeners.isEmpty())
                                   val = get(key);
                           _root.remove(key);
                               if(val!=null)
                                       for(RecordListener<K,V> r : recordListeners)
                                               r.recordRemoved((K)key,val);
                       }
		return oldVal;
	}catch (ClassCastException e){
		return null;
	}catch (IOException e){
		throw new IOError(e);
	}
    }

    public synchronized boolean containsKey(Object key) {
        if(key == null)
            return false;

        V v = get((K) key);
        return v!=null;
    }

    public synchronized void clear(){
        try{
            Iterator<K> keyIter = keys();
            while(keyIter.hasNext())
                remove(keyIter.next());
        }catch(IOException e){
            throw new IOError(e);
        }
    }


    /**
     * Returns an enumeration of the keys contained in this
     */
    public synchronized Iterator<K> keys()
        throws IOException
    {
        return _root.keys();
    }

    /**
     * Get the record identifier used to load this hashtable.
     */
    public long getRecid()
    {
        return _root.getRecid();
    }


    public RecordManager getRecordManager() {
        return _root.getRecordManager();
    }

    /**
     * add RecordListener which is notified about record changes
     * @param listener
     */
    public void addRecordListener(RecordListener<K,V> listener){
    	recordListeners.add(listener);
    }

    /**
     * remove RecordListener which is notified about record changes
     * @param listener
     */
    public void removeRecordListener(RecordListener<K,V> listener){
    	recordListeners.remove(listener);
    }


    public Set<Entry<K, V>> entrySet() {
            return _entrySet;
    }

    private Set<Entry<K, V>> _entrySet = new AbstractSet<Entry<K,V>>(){

                    protected Entry<K,V> newEntry(K k,V v){
                            return new SimpleEntry<K,V>(k,v){
                                    private static final long serialVersionUID = 978651696969194154L;

                                    public V setValue(V arg0) {
                                            HTree.this.put(getKey(), arg0);
                                            return super.setValue(arg0);
                                    }

                            };
                    }

                    public boolean add(java.util.Map.Entry<K, V> e) {
                            if(readonly)
                                    throw new UnsupportedOperationException("readonly");

                                    if(e.getKey() == null)
                                            throw new NullPointerException("Can not add null key");
                                    if(e.getValue().equals(get(e.getKey())))
                                                    return false;
                                    HTree.this.put(e.getKey(), e.getValue());
                                    return true;
                    }

                    @SuppressWarnings("unchecked")
                    public boolean contains(Object o) {
                            if(o instanceof Entry){
                                    Entry<K,V> e = (java.util.Map.Entry<K, V>) o;

                                    if(e.getKey()!=null && HTree.this.get(e.getKey())!=null)
                                        return true;
                            }
                            return false;
                    }


                    public Iterator<java.util.Map.Entry<K, V>> iterator() {
                            try {
                                final Iterator<K> br = keys();
                                return new Iterator<Entry<K,V>>(){

                                    private Entry<K,V> next;
                                    private K lastKey;
                                    void ensureNext(){
                                            if(br.hasNext()){
                                                   K k = br.next();
                                                   next = newEntry(k,get(k));
                                            }else
                                                   next = null;
                                    }
                                    {
                                            ensureNext();
                                    }



                                    public boolean hasNext() {
                                            return next!=null;
                                    }

                                    public java.util.Map.Entry<K, V> next() {
                                            if(next == null)
                                                    throw new NoSuchElementException();
                                            Entry<K,V> ret = next;
                                            lastKey = ret.getKey();
                                            //move to next position
                                            ensureNext();
                                            return ret;
                                    }

                                    public void remove() {
                                            if(readonly)
                                                    throw new UnsupportedOperationException("readonly");
                                            if(lastKey == null)
                                                    throw new IllegalStateException();

                                                    HTree.this.remove(lastKey);
                                                    lastKey = null;
                                    }};

                            } catch (IOException e) {
                                    throw new IOError(e);
                            }

                    }

                    @SuppressWarnings("unchecked")
                    public boolean remove(Object o) {
                            if(readonly)
                                    throw new UnsupportedOperationException("readonly");

                            if(o instanceof Entry){
                                    Entry<K,V> e = (java.util.Map.Entry<K, V>) o;

                                        //check for nulls
                                            if(e.getKey() == null || e.getValue() == null)
                                                    return false;
                                            //get old value, must be same as item in entry
                                            V v = get(e.getKey());
                                            if(v == null || !e.getValue().equals(v))
                                                    return false;
                                            HTree.this.remove(e.getKey());
                                            return true;
                            }
                            return false;

                    }

                    @Override
                    public int size() {
                            try{
                                    int counter = 0;
                                    Iterator<K> it = keys();
                                    while(it.hasNext()){
                                            it.next();
                                            counter ++;
                                    }
                                    return counter;
                            }catch (IOException e){
                                    throw new IOError(e);
                            }

                    }

            };



}

