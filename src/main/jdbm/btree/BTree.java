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

package jdbm.btree;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import jdbm.RecordListener;
import jdbm.RecordManager;
import jdbm.Serializer;
import jdbm.helper.ComparableComparator;
import jdbm.helper.JdbmBase;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

/**
 * B+Tree persistent indexing data structure.  B+Trees are optimized for
 * block-based, random I/O storage because they store multiple keys on
 * one tree node (called <code>BPage</code>).  In addition, the leaf nodes
 * directly contain (inline) the values associated with the keys, allowing a
 * single (or sequential) disk read of all the values on the page.
 * <p>
 * B+Trees are n-airy, yeilding log(N) search cost.  They are self-balancing,
 * preventing search performance degradation when the size of the tree grows.
 * <p>
 * Keys and associated values must be <code>Serializable</code> objects. The
 * user is responsible to supply a serializable <code>Comparator</code> object
 * to be used for the ordering of entries, which are also called <code>Tuple</code>.
 * The B+Tree allows traversing the keys in forward and reverse order using a
 * TupleBrowser obtained from the browse() methods.
 * <p>
 * This implementation does not directly support duplicate keys, but it is
 * possible to handle duplicates by inlining or referencing an object collection
 * as a value.
 * <p>
 * There is no limit on key size or value size, but it is recommended to keep
 * both as small as possible to reduce disk I/O.   This is especially true for
 * the key size, which impacts all non-leaf <code>BPage</code> objects.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: BTree.java,v 1.6 2005/06/25 23:12:31 doomdark Exp $
 */
public class BTree<K,V> 
    implements Externalizable, JdbmBase<K,V>
{

    private static final long serialVersionUID = 8883213742777032628L;

    private static final boolean DEBUG = false;



    /**
     * Default page size (number of entries per node)
     */
    public static final int DEFAULT_SIZE = 32;


    /**
     * Page manager used to persist changes in BPages
     */
    protected transient RecordManager _recman;


    /**
     * This BTree's record ID in the PageManager.
     */
    private transient long _recid;


    /**
     * Comparator used to index entries.
     */
    protected Comparator<K> _comparator;


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


	public void setKeySerializer(Serializer<K> keySerializer) {
		this.keySerializer = keySerializer;
	}


	public Serializer<V> getValueSerializer() {
		return valueSerializer;
	}


	public void setValueSerializer(Serializer<V> valueSerializer) {
		this.valueSerializer = valueSerializer;
	}

	/**
     * Height of the B+Tree.  This is the number of BPages you have to traverse
     * to get to a leaf BPage, starting from the root.
     */
    private int _height;


    /**
     * Recid of the root BPage
     */
    private transient long _root;


    /**
     * Number of entries in each BPage.
     */
    protected int _pageSize;


    /**
     * Total number of entries in the BTree
     */
    protected volatile int _entries;

    
    /**
     * Serializer used for BPages of this tree
     */
    private transient BPage<K,V> _bpageSerializer;
    
    
    /**
     * Listeners which are notified about changes in records
     */
    protected List<RecordListener<K,V>> recordListeners = new CopyOnWriteArrayList<RecordListener<K, V>>();
    
    protected ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * No-argument constructor used by serialization.
     */
    public BTree()
    {
        // empty
    }


    /**
     * Create a new persistent BTree, with 16 entries per node.
     *
     * @param recman Record manager used for persistence.
     * @param comparator Comparator used to order index entries
     */
    public static <K,V> BTree<K,V> createInstance( RecordManager recman,
                                        Comparator<K> comparator)
        throws IOException
    {
        return createInstance( recman, comparator, null, null, DEFAULT_SIZE );
    }
    
    /**
     * Create a new persistent BTree, with 16 entries per node.
     *
     * @param recman Record manager used for persistence.
     * @param comparator Comparator used to order index entries
     */
    @SuppressWarnings("unchecked")
	public static <K extends Comparable,V> BTree<K,V> createInstance( RecordManager recman)
        throws IOException
    {
    	BTree<K,V> ret = createInstance( recman, ComparableComparator.INSTANCE, null, null, DEFAULT_SIZE ); 
        return ret;
    }



    /**
     * Create a new persistent BTree, with 16 entries per node.
     *
     * @param recman Record manager used for persistence.
     * @param keySerializer Serializer used to serialize index keys (optional)
     * @param valueSerializer Serializer used to serialize index values (optional)
     * @param comparator Comparator used to order index entries
     */
    public static <K,V> BTree<K,V> createInstance( RecordManager recman,
                                        Comparator<K> comparator,
                                        Serializer<K> keySerializer,
                                        Serializer<V> valueSerializer )
        throws IOException
    {
        return createInstance( recman, comparator, keySerializer, 
                               valueSerializer, DEFAULT_SIZE );
    }


    /**
     * Create a new persistent BTree with the given number of entries per node.
     *
     * @param recman Record manager used for persistence.
     * @param comparator Comparator used to order index entries
     * @param keySerializer Serializer used to serialize index keys (optional)
     * @param valueSerializer Serializer used to serialize index values (optional)
     * @param pageSize Number of entries per page (must be even).
     */
    public static <K,V> BTree<K,V> createInstance( RecordManager recman,
                                        Comparator<K> comparator,
                                        Serializer<K> keySerializer,
                                        Serializer<V> valueSerializer,
                                        int pageSize )
        throws IOException
    {
        BTree<K,V> btree;

        if ( recman == null ) {
            throw new IllegalArgumentException( "Argument 'recman' is null" );
        }

        if ( comparator == null ) {
            throw new IllegalArgumentException( "Argument 'comparator' is null" );
        }
        if ( ! ( comparator instanceof Serializable ) ) {
            throw new IllegalArgumentException( "Argument 'comparator' must be serializable" );
        }


        // make sure there's an even number of entries per BPage
        if ( ( pageSize & 1 ) != 0 ) {
            throw new IllegalArgumentException( "Argument 'pageSize' must be even" );
        }

        btree = new BTree<K,V>();
        btree._recman = recman;
        btree._comparator = comparator;
        btree.keySerializer = keySerializer;
        btree.valueSerializer = valueSerializer;
        btree._pageSize = pageSize;
        btree._bpageSerializer = new BPage<K,V>();
        btree._bpageSerializer._btree = btree;
        btree._recid = recman.insert( btree );
        return btree;
    }


    /**
     * Load a persistent BTree.
     *
     * @param recman RecordManager used to store the persistent btree
     * @param recid Record id of the BTree
     */
    @SuppressWarnings("unchecked")
	public static <K,V> BTree<K,V> load( RecordManager recman, long recid )
        throws IOException
    {
        BTree<K,V> btree = (BTree<K,V>) recman.fetch( recid );
        btree._recid = recid;
        btree._recman = recman;
        btree._bpageSerializer = new BPage<K,V>();
        btree._bpageSerializer._btree = btree;
        return btree;
    }
    
    /**
     * Get the {@link ReadWriteLock} associated with this BTree.
     * This should be used with browsing operations to ensure
     * consistency.
     * @return
     */
    public ReadWriteLock getLock() {
		return lock;
	}

    /**
     * Insert an entry in the BTree.
     * <p>
     * The BTree cannot store duplicate entries.  An existing entry can be
     * replaced using the <code>replace</code> flag.   If an entry with the
     * same key already exists in the BTree, its value is returned.
     *
     * @param key Insert key
     * @param value Insert value
     * @param replace Set to true to replace an existing key-value pair.
     * @return Existing value, if any.
     */
    public V insert(final K key, final V value,
                                       final boolean replace )
        throws IOException
    {
        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }
        if ( value == null ) {
            throw new IllegalArgumentException( "Argument 'value' is null" );
        }
        try {
        	lock.writeLock().lock();
	        BPage<K,V> rootPage = getRoot();
	
	        if ( rootPage == null ) {
	            // BTree is currently empty, create a new root BPage
	            if (DEBUG) {
	                System.out.println( "BTree.insert() new root BPage" );
	            }
	            rootPage = new BPage<K,V>( this, key, value );
	            _root = rootPage._recid;
	            _height = 1;
	            _entries = 1;
	            _recman.update( _recid, this );
	            //notifi listeners
	            for(RecordListener<K,V> l : recordListeners){
	            	l.recordInserted(key, value);
	            }
	            return null;
            } else {
	        BPage.InsertResult<K,V> insert = rootPage.insert( _height, key, value, replace );
            boolean dirty = false;
            if ( insert._overflow != null ) {
                // current root page overflowed, we replace with a new root page
                if ( DEBUG ) {
                    System.out.println( "BTree.insert() replace root BPage due to overflow" );
                }
                rootPage = new BPage<K,V>( this, rootPage, insert._overflow );
                _root = rootPage._recid;
                _height += 1;
                dirty = true;
            }
            if ( insert._existing == null ) {
                _entries++;
                dirty = true;
            }
            if ( dirty ) {
                _recman.update( _recid, this );
            }
            //notify listeners
            for(RecordListener<K,V> l : recordListeners){
            	if(insert._existing==null)
            		l.recordInserted(key, value);
            	else
            		l.recordUpdated(key, insert._existing, value);
            }

            // insert might have returned an existing value
            return insert._existing;
           }
        } finally {
        	lock.writeLock().unlock();
        }
    }


    /**
     * Remove an entry with the given key from the BTree.
     *
     * @param key Removal key
     * @return Value associated with the key, or null if no entry with given
     *         key existed in the BTree.
     */
    public V remove( K key )
        throws IOException
    {
        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }
        try {
        	lock.writeLock().lock();
	        BPage<K,V> rootPage = getRoot();
	        if ( rootPage == null ) {
	            return null;
	        }
	        boolean dirty = false;
	        BPage.RemoveResult<K,V> remove = rootPage.remove( _height, key );
	        if ( remove._underflow && rootPage.isEmpty() ) {
	            _height -= 1;
	            dirty = true;
	
	            // TODO:  check contract for BPages to be removed from recman.
	            if ( _height == 0 ) {
	                _root = 0;
	            } else {
	                _root = rootPage.childBPage( _pageSize-1 )._recid;
	            }
	        }
	        if ( remove._value != null ) {
	            _entries--;
	            dirty = true;
	        }
	        if ( dirty ) {
	            _recman.update( _recid, this );
	        }
	        if(remove._value!=null)
	        	for(RecordListener<K,V> l : recordListeners)
	        		l.recordRemoved(key,remove._value);
	        return remove._value;
        } finally {
        	lock.writeLock().unlock();
        }
    }


    /**
     * Find the value associated with the given key.
     *
     * @param key Lookup key.
     * @return Value associated with the key, or null if not found.
     */
    public V find( K key )
        throws IOException
    {
        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }
        try {
        	lock.readLock().lock();
	        BPage<K,V> rootPage = getRoot();
	        if ( rootPage == null ) {
	            return null;
	        }
	
	        return rootPage.findValue( _height, key );
        } finally {
        	lock.readLock().unlock();
        }
//        Tuple<K,V> tuple = new Tuple<K,V>( null, null );
//        TupleBrowser<K,V> browser = rootPage.find( _height, key );
//
//        if ( browser.getNext( tuple ) ) {
//            // find returns the matching key or the next ordered key, so we must
//            // check if we have an exact match
//            if ( _comparator.compare( key, tuple.getKey() ) != 0 ) {
//                return null;
//            } else {
//                return tuple.getValue();
//            }
//        } else {
//            return null;
//        }
    }


    /**
     * Find the value associated with the given key, or the entry immediately
     * following this key in the ordered BTree.
     *
     * @param key Lookup key.
     * @return Value associated with the key, or a greater entry, or null if no
     *         greater entry was found.
     */
    public Tuple<K,V> findGreaterOrEqual( K key )
        throws IOException
    {
        Tuple<K,V>         tuple;
        TupleBrowser<K,V>  browser;

        if ( key == null ) {
            // there can't be a key greater than or equal to "null"
            // because null is considered an infinite key.
            return null;
        }

        tuple = new Tuple<K,V>( null, null );
        browser = browse( key );
        if ( browser.getNext( tuple ) ) {
            return tuple;
        } else {
            return null;
        }
    }


    /**
     * Get a browser initially positioned at the beginning of the BTree.
     * <p><b>
     * WARNING: �If you make structural modifications to the BTree during
     * browsing, you will get inconsistent browing results.
     * </b>
     *
     * @return Browser positionned at the beginning of the BTree.
     */
    @SuppressWarnings("unchecked")
	public TupleBrowser<K,V> browse()
        throws IOException
    {
    	try {
        	lock.readLock().lock();
	        BPage<K,V> rootPage = getRoot();
	        if ( rootPage == null ) {
	            return EmptyBrowser.INSTANCE;
	        }
	        TupleBrowser<K,V> browser = rootPage.findFirst();
	        return browser;
    	} finally {
    		lock.readLock().unlock();
    	}
    }


    /**
     * Get a browser initially positioned just before the given key.
     * <p><b>
     * WARNING: �If you make structural modifications to the BTree during
     * browsing, you will get inconsistent browing results.
     * </b>
     *
     * @param key Key used to position the browser.  If null, the browser
     *            will be positionned after the last entry of the BTree.
     *            (Null is considered to be an "infinite" key)
     * @return Browser positionned just before the given key.
     */
    @SuppressWarnings("unchecked")
	public TupleBrowser<K,V> browse( K key )
        throws IOException
    {
    	try {
        	lock.readLock().lock();
	    	BPage<K,V> rootPage = getRoot();
	        if ( rootPage == null ) {
	            return EmptyBrowser.INSTANCE;
	        }
	        TupleBrowser<K,V> browser = rootPage.find( _height, key );
	        return browser;
    	} finally {
    		lock.readLock().unlock();
    	}
    }


    /**
     * Return the number of entries (size) of the BTree.
     */
    public int size()
    {
        return _entries;
    }


    /**
     * Return the persistent record identifier of the BTree.
     */
    public long getRecid()
    {
        return _recid;
    }


    /**
     * Return the root BPage, or null if it doesn't exist.
     */
    BPage<K,V> getRoot()
        throws IOException
    {
        if ( _root == 0 ) {
            return null;
        }
        BPage<K,V> root = (BPage<K,V>) _recman.fetch( _root, _bpageSerializer );
        if (root != null) {
            root._recid = _root;
            root._btree = this;
        }
        return root;
    }

    /**
     * Implement Externalizable interface.
     */
    @SuppressWarnings("unchecked")
	public void readExternal( ObjectInput in )
        throws IOException, ClassNotFoundException
    {
        _comparator = (Comparator<K>) in.readObject();
      //serializer is not persistent from 2.0        
//        _keySerializer = (Serializer<K>) in.readObject();
//        _valueSerializer = (Serializer<V>) in.readObject();
        _height = in.readInt();
        _root = in.readLong();
        _pageSize = in.readInt();
        _entries = in.readInt();
    }


    /**
     * Implement Externalizable interface.
     */
    public void writeExternal( ObjectOutput out )
        throws IOException
    {
        out.writeObject( _comparator );
        //serializer is not persistent from 2.0         
//        out.writeObject( _keySerializer );
//        out.writeObject( _valueSerializer );
        out.writeInt( _height );
        out.writeLong( _root );
        out.writeInt( _pageSize );
        out.writeInt( _entries );
    }


    /*
    public void assert() throws IOException {
        BPage root = getRoot();
        if ( root != null ) {
            root.assertRecursive( _height );
        }
    }
    */


    /*
    public void dump() throws IOException {
        BPage root = getRoot();
        if ( root != null ) {
            root.dumpRecursive( _height, 0 );
        }
    }
    */


    /** PRIVATE INNER CLASS
     *  Browser returning no element.
     */
    static class EmptyBrowser<K,V>
    	implements TupleBrowser<K,V>    {

        @SuppressWarnings("unchecked")
		static TupleBrowser INSTANCE = new EmptyBrowser();
        
        private EmptyBrowser(){}

        public boolean getNext( Tuple<K,V> tuple )
        {
            return false;
        }

        public boolean getPrevious( Tuple<K,V> tuple )
        {
            return false;
        }
    }
    
    public BTreeSortedMap<K,V> asMap(){
    	return new BTreeSortedMap<K, V>(this,false);
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
		return _recman;
	}
	

    public Comparator<K> getComparator() {
        return _comparator;
    }

    /** 
     * Deletes all BPages in this BTree, then deletes the tree from the record manager
     */
    public void delete()
        throws IOException
    {
    	try {
        	lock.writeLock().lock();
	        BPage<K,V> rootPage = getRoot();
	        if (rootPage != null)
	            rootPage.delete();
	        _recman.delete(_recid);
    	} finally {
    		lock.writeLock().unlock();
    	}
    }
    
    /**
     * Used for debugging and testing only.  Populates the 'out' list with
     * the recids of all child pages in the BTree.
     * @param out
     * @throws IOException
     */
    void dumpChildPageRecIDs(List<Long> out) throws IOException{
        BPage<K,V> root = getRoot();
        if ( root != null ) {
            out.add(root._recid);
            root.dumpChildPageRecIDs( out, _height);
        }
    }
 }

