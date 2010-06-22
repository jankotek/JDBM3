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

import jdbm.RecordListener;
import jdbm.RecordManager;
import jdbm.helper.JdbmBase;

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

    /**
     * Root hash directory.
     */
    private HashDirectory<K,V> _root;


    /**
     * Listeners which are notified about changes in records
     */
    protected List<RecordListener<K,V>> recordListeners = new ArrayList<RecordListener<K,V>>();

    
    /**
     * Private constructor
     *
     * @param root Root hash directory.
     */
    private HTree( HashDirectory<K,V> root ) {
        _root = root;
    }


    /**
     * Create a persistent hashtable.
     *
     * @param recman Record manager used for persistence.
     */
    public static <K,V> HTree<K,V> createInstance( RecordManager recman )
        throws IOException
    {
        HashDirectory<K,V>  root;
        long           recid;

        root = new HashDirectory<K,V>( (byte) 0 );
        recid = recman.insert( root,HashNode.SERIALIZER  );
        root.setPersistenceContext( recman, recid );

        return new HTree<K,V>( root );
    }


    /**
     * Load a persistent hashtable
     *
     * @param recman RecordManager used to store the persistent hashtable
     * @param root_recid Record id of the root directory of the HTree
     */
    public static <K,V>  HTree<K,V> load( RecordManager recman, long root_recid )
        throws IOException
    {
        HTree<K,V> tree;
        HashDirectory<K,V> root;

        root = (HashDirectory<K,V>) recman.fetch( root_recid,HashNode.SERIALIZER  );
        root.setPersistenceContext( recman, root_recid );
        tree = new HTree<K,V>( root );
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

