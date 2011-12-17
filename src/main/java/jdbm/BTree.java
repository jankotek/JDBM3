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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
 * @author Alex Boisvert
 */
class BTree<K,V>
    implements  JdbmBase<K,V>
{


    private static final boolean DEBUG = false;



    /**
     * Default page size (number of entries per node)
     */
    public static final int DEFAULT_SIZE = 8;


    /**
     * Page manager used to persist changes in BPages
     */
    protected transient RecordManager2 _recman;


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

    /** indicates if values should be loaded during deserialization, set to true during defragmentation */
    boolean loadValues = true;



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
     * Total number of entries in the BTree
     */
    protected volatile int _entries;

    
    /**
     * Serializer used for BPages of this tree
     */
    private transient BTreePage<K,V> _bpageSerializer;
    
    
    /**
     * Listeners which are notified about changes in records
     */
    protected RecordListener[] recordListeners =new  RecordListener[0];
    
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
    public static <K,V> BTree<K,V> createInstance( RecordManager2 recman,
                                        Comparator<K> comparator)
        throws IOException
    {
        return createInstance( recman, comparator, null, null);
    }
    
    /**
     * Create a new persistent BTree, with 16 entries per node.
     *
     * @param recman Record manager used for persistence.
     */
    @SuppressWarnings("unchecked")
	public static <K extends Comparable,V> BTree<K,V> createInstance( RecordManager2 recman)
        throws IOException
    {
    	BTree<K,V> ret = createInstance( recman, null, null, null);
        return ret;
    }



    /**
     * Create a new persistent BTree with the given number of entries per node.
     *
     * @param recman Record manager used for persistence.
     * @param comparator Comparator used to order index entries
     * @param keySerializer Serializer used to serialize index keys (optional)
     * @param valueSerializer Serializer used to serialize index values (optional)
     */
    public static <K,V> BTree<K,V> createInstance( RecordManager2 recman,
                                        Comparator<K> comparator,
                                        Serializer<K> keySerializer,
                                        Serializer<V> valueSerializer)
        throws IOException
    {
        BTree<K,V> btree;

        if ( recman == null ) {
            throw new IllegalArgumentException( "Argument 'recman' is null" );
        }

        btree = new BTree<K,V>();
        btree._recman = recman;
        btree._comparator = comparator;
        btree.keySerializer = keySerializer;
        btree.valueSerializer = valueSerializer;
        btree._bpageSerializer = new BTreePage<K,V>();
        btree._bpageSerializer._btree = btree;
        btree._recid = recman.insert( btree, btree.getRecordManager().defaultSerializer() );
        return btree;
    }


    /**
     * Load a persistent BTree.
     *
     * @param recman RecordManager used to store the persistent btree
     * @param recid Record id of the BTree
     */
    @SuppressWarnings("unchecked")
	public static <K,V> BTree<K,V> load( RecordManager2 recman, long recid )
        throws IOException
    {
        BTree<K,V> btree = (BTree<K,V>) recman.fetch( recid);
        btree._recid = recid;
        btree._recman = recman;
        btree._bpageSerializer = new BTreePage<K,V>();
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
	        BTreePage<K,V> rootPage = getRoot();
	
	        if ( rootPage == null ) {
	            // BTree is currently empty, create a new root BPage
	            if (DEBUG) {
	                System.out.println( "BTree.insert() new root BPage" );
	            }
	            rootPage = new BTreePage<K,V>( this, key, value );
	            _root = rootPage._recid;
	            _height = 1;
	            _entries = 1;
	            _recman.update( _recid, this);
	            //notifi listeners
	            for(RecordListener<K,V> l : recordListeners){
	            	l.recordInserted(key, value);
	            }
	            return null;
            } else {
	        BTreePage.InsertResult<K,V> insert = rootPage.insert( _height, key, value, replace );
            boolean dirty = false;
            if ( insert._overflow != null ) {
                // current root page overflowed, we replace with a new root page
                if ( DEBUG ) {
                    System.out.println( "BTree.insert() replace root BPage due to overflow" );
                }
                rootPage = new BTreePage<K,V>( this, rootPage, insert._overflow );
                _root = rootPage._recid;
                _height += 1;
                dirty = true;
            }
            if ( insert._existing == null ) {
                _entries++;
                dirty = true;
            }
            if ( dirty ) {
                _recman.update( _recid, this);
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
	        BTreePage<K,V> rootPage = getRoot();
	        if ( rootPage == null ) {
	            return null;
	        }
	        boolean dirty = false;
	        BTreePage.RemoveResult<K,V> remove = rootPage.remove( _height, key );
	        if ( remove._underflow && rootPage.isEmpty() ) {
	            _height -= 1;
	            dirty = true;
	
                _recman.delete(_root);
	            if ( _height == 0 ) {
	                _root = 0;
	            } else {
	                _root = rootPage.loadLastChildPage()._recid;
	            }
	        }
	        if ( remove._value != null ) {
	            _entries--;
	            dirty = true;
	        }
	        if ( dirty ) {
	            _recman.update( _recid, this);
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
    public V get(K key)
        throws IOException
    {
        if ( key == null ) {
            throw new IllegalArgumentException( "Argument 'key' is null" );
        }
        try {
        	lock.readLock().lock();
	        BTreePage<K,V> rootPage = getRoot();
	        if ( rootPage == null ) {
	            return null;
	        }
	
	        return rootPage.findValue( _height, key );
        } finally {
        	lock.readLock().unlock();
        }
//        Tuple<K,V> tuple = new Tuple<K,V>( null, null );
//        TupleBrowser<K,V> browser = rootPage.get( _height, key );
//
//        if ( browser.getNext( tuple ) ) {
//            // get returns the matching key or the next ordered key, so we must
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
    public BTreeTuple<K,V> findGreaterOrEqual( K key )
        throws IOException
    {
        BTreeTuple<K,V> tuple;
        BTreeTupleBrowser<K,V> browser;

        if ( key == null ) {
            // there can't be a key greater than or equal to "null"
            // because null is considered an infinite key.
            return null;
        }

        tuple = new BTreeTuple<K,V>( null, null );
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
     * WARNING: If you make structural modifications to the BTree during
     * browsing, you will get inconsistent browing results.
     * </b>
     *
     * @return Browser positionned at the beginning of the BTree.
     */
    @SuppressWarnings("unchecked")
	public BTreeTupleBrowser<K,V> browse()
        throws IOException
    {
    	try {
        	lock.readLock().lock();
	        BTreePage<K,V> rootPage = getRoot();
	        if ( rootPage == null ) {
	            return EmptyBrowser.INSTANCE;
	        }
	        BTreeTupleBrowser<K,V> browser = rootPage.findFirst();
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
	public BTreeTupleBrowser<K,V> browse( K key )
        throws IOException
    {
    	try {
        	lock.readLock().lock();
	    	BTreePage<K,V> rootPage = getRoot();
	        if ( rootPage == null ) {
	            return EmptyBrowser.INSTANCE;
	        }
	        BTreeTupleBrowser<K,V> browser = rootPage.find( _height, key );
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
    BTreePage<K,V> getRoot()
        throws IOException
    {
        if ( _root == 0 ) {
            return null;
        }
        BTreePage<K,V> root = (BTreePage<K,V>) _recman.fetch( _root, _bpageSerializer );
        if (root != null) {
            root._recid = _root;
            root._btree = this;
        }
        return root;
    }


	static BTree readExternal( DataInput in)
        throws IOException, ClassNotFoundException
    {
        BTree tree = new BTree();
        tree._height = in.readInt();
        tree._root = in.readLong();
        tree._entries = in.readInt();
        tree._comparator = (Comparator)Utils.CONSTRUCTOR_SERIALIZER.deserialize(in);
        tree.keySerializer = (Serializer)Utils.CONSTRUCTOR_SERIALIZER.deserialize(in);
        tree.valueSerializer = (Serializer)Utils.CONSTRUCTOR_SERIALIZER.deserialize(in);
        return tree;
    }


    public void writeExternal( DataOutput out )
        throws IOException
    {
        out.writeInt( _height );
        out.writeLong( _root );
        out.writeInt( _entries );

        Utils.CONSTRUCTOR_SERIALIZER.serialize(out,_comparator);
        Utils.CONSTRUCTOR_SERIALIZER.serialize(out,keySerializer);
        Utils.CONSTRUCTOR_SERIALIZER.serialize(out,valueSerializer);
    }

    public static void defrag(long recid, RecordManagerStorage r1, RecordManagerStorage r2) throws IOException {
        try{
            byte[] data = r1.fetchRaw(recid);
            r2.forceInsert(recid,data);
            DataInput in = new DataInputOutput(data);
            BTree t = (BTree) r1.defaultSerializer().deserialize(in);
            t.loadValues = false;
            t._recman = r1;
            t._bpageSerializer = new BTreePage(t,false);


            BTreePage p = t.getRoot();
            if(p!=null){
                r2.forceInsert(t._root,r1.fetchRaw(t._root));
                p.defrag(r1,r2);
            }

        }catch(ClassNotFoundException e){
            throw new IOError(e);
        }
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
    	implements BTreeTupleBrowser<K,V> {

        @SuppressWarnings("unchecked")
		static BTreeTupleBrowser INSTANCE = new EmptyBrowser();
        
        private EmptyBrowser(){}

        public boolean getNext( BTreeTuple<K,V> tuple )
        {
            return false;
        }

        public boolean getPrevious( BTreeTuple<K,V> tuple )
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
        recordListeners = Arrays.copyOf(recordListeners, recordListeners.length + 1);
    	recordListeners[recordListeners.length-1]=listener;
    }

    /**
     * remove RecordListener which is notified about record changes
     * @param listener
     */
    public void removeRecordListener(RecordListener<K,V> listener){
        List l = Arrays.asList(recordListeners);
        l.remove(listener);
    	recordListeners = (RecordListener[]) l.toArray(new RecordListener[1]);
    }



	public RecordManager2 getRecordManager() {
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
	        BTreePage<K,V> rootPage = getRoot();
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
        BTreePage<K,V> root = getRoot();
        if ( root != null ) {
            out.add(root._recid);
            root.dumpChildPageRecIDs( out, _height);
        }
    }

    /**
     * Tuple consisting of a key-value pair.
     *
     * @author Alex Boisvert
     */
    static final class BTreeTuple<K,V> {

        K key;

        V value;

        BTreeTuple() {
            // empty
        }

        BTreeTuple(K key, V value) {
            this.key = key;
            this.value = value;
        }

    }

    /**
     * Browser to traverse a collection of tuples.  The browser allows for
     * forward and reverse order traversal.
     *
     * @author Alex Boisvert
     */
    static interface BTreeTupleBrowser<K,V> {

        /**
         * Get the next tuple.
         *
         * @param tuple Tuple into which values are copied.
         * @return True if values have been copied in tuple, or false if there is no next tuple.
         */
        public abstract boolean getNext(BTreeTuple<K, V> tuple)throws IOException;

        /**
         * Get the previous tuple.
         *
         * @param tuple Tuple into which values are copied.
         * @return True if values have been copied in tuple, or false if there is no previous tuple.
         */
        public abstract boolean getPrevious(BTreeTuple<K, V> tuple) throws IOException;

    }
}

