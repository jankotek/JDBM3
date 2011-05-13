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

package jdbm.htree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import jdbm.*;
import jdbm.helper.JdbmBase;
import jdbm.helper.Serialization;

/**
 *  Persistent hashtable implementation for PageManager.
 *  Implemented as an H*Tree structure.
 *
 *  WARNING!  If this instance is used in a transactional context, it
 *            *must* be discarded after a rollback.
 *
 *  @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 *  @version $Id: HTree.java,v 1.3 2005/06/25 23:12:32 doomdark Exp $
 */
public class HTree<K,V> implements JdbmBase<K,V>
{

    final Serializer SERIALIZER = new Serializer<HashNode>() {

        public HashNode deserialize(SerializerInput ds) throws IOException {
            try{
                int i = ds.read();
                if(i == Serialization.HTREE_BUCKET){ //is HashBucket?
                    HashBucket ret = new HashBucket(HTree.this);
                    ret.readExternal(ds);
                    if(ds.available()!=0 && ds.read()!=-1) // -1 is fix for compression, not sure what is happening
                        throw new InternalError("bytes left: "+ds.available());
                    return ret;
                }else if( i == Serialization.HTREE_DIRECTORY){
                    HashDirectory ret = new HashDirectory(HTree.this);
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
        public void serialize(SerializerOutput out, HashNode obj) throws IOException {
            if(obj.getClass() ==  HashBucket.class){
                out.write(Serialization.HTREE_BUCKET);
                HashBucket b = (HashBucket) obj;
                b.writeExternal(out);
            }else{
                out.write(Serialization.HTREE_DIRECTORY);
                HashDirectory n = (HashDirectory) obj;
                n.writeExternal(out);
            }
        }
    };


    /**
     * Root hash directory.
     */
    private HashDirectory<K,V> _root;


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
    public static <K,V> HTree<K,V> createInstance( RecordManager recman )
        throws IOException
    {
        return createInstance(recman, null, null);
    }

    /**
     * Create a persistent hashtable.
     *
     * @param recman Record manager used for persistence.
     */
    public static <K,V> HTree<K,V> createInstance( RecordManager recman,
                                                   Serializer<K> keySerializer,
                                                   Serializer<V> valueSerializer )
        throws IOException
    {
        HashDirectory<K,V>  root;
        long           recid;

        HTree<K,V> tree = new HTree<K,V>();
        tree.keySerializer = keySerializer;
        tree.valueSerializer = valueSerializer;

        tree._root = new HashDirectory<K,V>( tree, (byte) 0 );
        recid = recman.insert( tree._root, tree.SERIALIZER );
        tree._root.setPersistenceContext(recman, recid);

        return tree;
    }


    /**
     * Load a persistent hashtable
     *
     * @param recman RecordManager used to store the persistent hashtable
     * @param root_recid Record id of the root directory of the HTree
     */
    public static <K,V>  HTree<K,V> load( RecordManager recman,
                                          long root_recid)
    throws IOException
    {
        return load(recman, root_recid, null, null);
    }
    /**
     * Load a persistent hashtable
     *
     * @param recman RecordManager used to store the persistent hashtable
     * @param root_recid Record id of the root directory of the HTree
     */
    public static <K,V>  HTree<K,V> load( RecordManager recman,
                                          long root_recid,
                                          Serializer<K> keySerializer,
                                          Serializer<V> valueSerializer )
        throws IOException
    {
        HashDirectory<K,V> root;

        HTree<K,V> tree = new HTree<K,V>();
        tree.keySerializer = keySerializer;
        tree.valueSerializer = valueSerializer;

        tree._root = (HashDirectory<K,V>) recman.fetch( root_recid, tree.SERIALIZER  );
        tree._root.setPersistenceContext( recman, root_recid );

        return tree;
    }

    transient long hashEqualsIdentityCounter=0;
    
    /**
     * Associates the specified value with the specified key.
     *
     * @param key key with which the specified value is to be assocated.
     * @param value value to be associated with the specified key.
     */
    public synchronized void put(K key, V value)
        throws IOException
    {
    	V oldVal = null;
    	if(!recordListeners.isEmpty())
    		oldVal = find(key);
    	
        _root.put(key, value);
        
        if(oldVal == null){
        	for(RecordListener<K,V> r : recordListeners)
        		r.recordInserted(key,value);
        }else{
        	for(RecordListener<K,V> r : recordListeners)
        		r.recordUpdated(key,oldVal,value);
        }        	
    }


    /**
     * Returns the value which is associated with the given key. Returns
     * <code>null</code> if there is not association for this key.
     *
     * @param key key whose associated value is to be returned
     */
    public synchronized V find(K key)
        throws IOException
    {
        return _root.get(key);
    }


    /**
     * Remove the value which is associated with the given key.  If the
     * key does not exist, this method simply ignores the operation.
     *
     * @param key key whose associated value is to be removed
     */
    public synchronized void remove(K key)
        throws IOException
    {
    	V val = null;
    	if(!recordListeners.isEmpty())
    		val = find(key);

        _root.remove(key);
    	if(val!=null)
    		for(RecordListener<K,V> r : recordListeners)
    			r.recordRemoved(key,val);

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
     * Returns an enumeration of the values contained in this
     */
    public synchronized Iterator<V> values()
        throws IOException
    {
        return _root.values();
    }


    /**
     * Get the record identifier used to load this hashtable.
     */
    public long getRecid()
    {
        return _root.getRecid();
    }
    
    public HTreeMap<K,V> asMap(){
    	return new HTreeMap<K,V>(this,false);
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


	public RecordManager getRecordManager() {
		return _root.getRecordManager();
	}


    


}

