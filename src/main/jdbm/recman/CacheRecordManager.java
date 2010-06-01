/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
 * Copyright 2000-2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 * $Id: CacheRecordManager.java,v 1.9 2005/06/25 23:12:32 doomdark Exp $
 */

package jdbm.recman;

import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;

import jdbm.RecordManager;
import jdbm.Serializer;
import jdbm.helper.DefaultSerializer;
import jdbm.helper.LongKeyChainedHashMap;

/**
 *  A RecordManager wrapping and caching another RecordManager.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @author <a href="cg@cdegroot.com">Cees de Groot</a>
 * @version $Id: CacheRecordManager.java,v 1.9 2005/06/25 23:12:32 doomdark Exp $
 */
public class CacheRecordManager
    extends RecordManager
{

    /**
     * Wrapped RecordManager
     */
    protected RecordManager _recman;


    /** Cached object hashtable */
    protected LongKeyChainedHashMap<CacheEntry> _hash;
	
    /** If Soft Cache is enabled, this contains softly referenced clean entries. 
     * If entry became dirty, it is moved to _hash with limited size.
     * This map is accessed from SoftCache Disposer thread, so all access must be 
     * synchronized  
     */
	protected LongKeyChainedHashMap<SoftCacheEntry> _softHash;

	/**
	 * Reference queue used to collect Soft Cache entries 
	 */
	protected ReferenceQueue<SoftCacheEntry> _refQueue;
	

    /**
     * Maximum number of objects in the cache.
     */
	protected int _max;
    
    /**
     * True if enable second level soft cache
     */
	protected boolean _softCache;
	
	/**
	 * Thread in which Soft Cache references are disposed
	 */
	protected Thread _softRefThread;
	
	protected static int threadCounter = 0;

    /**
     * Beginning of linked-list of cache elements.  First entry is element
     * which has been used least recently.
     */
	protected CacheEntry _first;

    /**
     * End of linked-list of cache elements.  Last entry is element
     * which has been used most recently.
     */
	protected CacheEntry _last;



    /**
     * Construct a CacheRecordManager wrapping another RecordManager and
     * using a given cache policy.
     *
     * @param recman Wrapped RecordManager
     * @param cache Cache policy
     */
    public CacheRecordManager( RecordManager recman, int maxRecords, boolean softCache)
    {
        if ( recman == null ) {
            throw new IllegalArgumentException( "Argument 'recman' is null" );
        }
        _hash = new LongKeyChainedHashMap<CacheEntry>(maxRecords);
        _recman = recman;
        _max = maxRecords;
        _softCache = softCache;
        
        if(softCache){
        	_softHash = new LongKeyChainedHashMap<SoftCacheEntry>(maxRecords);
        	_refQueue = new ReferenceQueue<SoftCacheEntry>();
        	_softRefThread = new Thread(
        			new SoftRunnable(this, _refQueue),
        			"JDBM Soft Cache Disposer "+(threadCounter++));
        	_softRefThread.setDaemon(true);
        	_softRefThread.start();
        }

    }

    
    /**
     * Get the underlying Record Manager.
     *
     * @return underlying RecordManager or null if CacheRecordManager has
     *         been closed. 
     */
    public RecordManager getRecordManager()
    {
        return _recman;
    }

    
    
    /**
     *  Inserts a new record using a custom serializer.
     *
     *  @param obj the object for the new record.
     *  @return the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public long insert( Object obj )
        throws IOException
    {
        return insert( obj, DefaultSerializer.INSTANCE );
    }
        
        
    /**
     *  Inserts a new record using a custom serializer.
     *
     *  @param obj the object for the new record.
     *  @param serializer a custom serializer
     *  @return the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized <A> long insert( A obj, Serializer<A> serializer )
        throws IOException
    {
        checkIfClosed();

        long recid = _recman.insert( obj, serializer );
        if(_softCache) synchronized(_softHash) {
        	_softHash.put(recid, new SoftCacheEntry(recid, obj, serializer,_refQueue));
        }else {
        	cachePut(  recid , obj, serializer, false );
        }
        return recid;
    }


    /**
     *  Deletes a record.
     *
     *  @param recid the rowid for the record that should be deleted.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void delete( long recid )
        throws IOException
    {
        checkIfClosed();

        _recman.delete( recid );
        CacheEntry entry = _hash.get(recid);
        if (entry != null) {
            removeEntry(entry);
            _hash.remove(entry._recid);
        }
        if(_softCache) synchronized(_softHash) {
        	SoftCacheEntry e = _softHash.remove(recid);
        	if(e!=null)
        		e.clear();
        }

    }


    /**
     *  Updates a record using standard Java serialization.
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param obj the new object for the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public void update( long recid, Object obj )
        throws IOException
    {
        update( recid, obj, DefaultSerializer.INSTANCE );
    }
    

    /**
     *  Updates a record using a custom serializer.
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param obj the new object for the record.
     *  @param serializer a custom serializer
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized <A> void update( long recid, A obj, 
                                     Serializer<A> serializer )
        throws IOException
    {       
        checkIfClosed();
        if(_softCache) synchronized(_softHash) {
        	//soft cache can not contain dirty objects
        	SoftCacheEntry e = _softHash.remove(recid);
        	if(e != null)
        		e.clear();
        }
        CacheEntry entry = cacheGet(recid);
        if ( entry != null ) {
            // reuse existing cache entry
            entry._obj = obj;
            entry._serializer = serializer;
            entry._isDirty = true;
        } else {
            cachePut( recid, obj, serializer, true );
        }
    }


    
    /**
     *  Fetches a record using standard Java serialization.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @return the object contained in the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public Object fetch( long recid )
        throws IOException
    {
        return fetch( recid, DefaultSerializer.INSTANCE );
    }

        
    /**
     *  Fetches a record using a custom serializer.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @param serializer a custom serializer
     *  @return the object contained in the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized <A> A fetch( long recid, Serializer<A> serializer )
        throws IOException
    {
        checkIfClosed();
        if(_softCache) synchronized(_softHash){
        	SoftCacheEntry e = _softHash.get(recid);
        	if(e!=null){
        		Object a = e.get();
        		if(a!=null) 
        			return (A) a;
        	}
        }

        CacheEntry entry = (CacheEntry) cacheGet( recid );
        if ( entry == null ) {
        	A value = _recman.fetch( recid, serializer );
        	cachePut(recid,value, serializer,false);
        	return value;
        }else{
        	return (A) entry._obj;
        }
    }


    /**
     *  Closes the record manager.
     *
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void close()
        throws IOException
    {
        checkIfClosed();

        updateCacheEntries();
        _recman.close();
        _recman = null;
        _hash = null;
        _softHash = null;
        if(_softCache)
        	_softRefThread.interrupt();
    }


    /**
     *  Returns the number of slots available for "root" rowids. These slots
     *  can be used to store special rowids, like rowids that point to
     *  other rowids. Root rowids are useful for bootstrapping access to
     *  a set of data.
     */
    public synchronized int getRootCount()
    {
        checkIfClosed();

        return _recman.getRootCount();
    }


    /**
     *  Returns the indicated root rowid.
     *
     *  @see #getRootCount
     */
    public synchronized long getRoot( int id )
        throws IOException
    {
        checkIfClosed();

        return _recman.getRoot( id );
    }


    /**
     *  Sets the indicated root rowid.
     *
     *  @see #getRootCount
     */
    public synchronized void setRoot( int id, long rowid )
        throws IOException
    {
        checkIfClosed();

        _recman.setRoot( id, rowid );
    }


    /**
     * Commit (make persistent) all changes since beginning of transaction.
     */
    public synchronized void commit()
        throws IOException
    {
        checkIfClosed();
        updateCacheEntries();
        _recman.commit();
    }


    /**
     * Rollback (cancel) all changes since beginning of transaction.
     */
    public synchronized void rollback()
        throws IOException
    {
        checkIfClosed();

        _recman.rollback();

        // discard all cache entries since we don't know which entries
        // where part of the transaction
    	_hash.clear();
    	if(_softCache) synchronized(_softHash) {
    		for(SoftCacheEntry e:_softHash.values())
    			e.clear();
    		_softHash.clear();
    	}
        _first = null;
        _last = null;
    }


    /**
     * Obtain the record id of a named object. Returns 0 if named object
     * doesn't exist.
     */
    public synchronized long getNamedObject( String name )
        throws IOException
    {
        checkIfClosed();

        return _recman.getNamedObject( name );
    }


    /**
     * Set the record id of a named object.
     */
    public synchronized void setNamedObject( String name, long recid )
        throws IOException
    {
        checkIfClosed();

        _recman.setNamedObject( name, recid );
    }


    /**
     * Check if RecordManager has been closed.  If so, throw an
     * IllegalStateException
     */
    private void checkIfClosed()
        throws IllegalStateException
    {
        if ( _recman == null ) {
            throw new IllegalStateException( "RecordManager has been closed" );
        }
    }

    
    /**
     * Update all dirty cache objects to the underlying RecordManager.
     */
    protected void updateCacheEntries()
        throws IOException
    {
        for(CacheEntry entry: _hash.values()){
            if ( entry._isDirty ) {
                _recman.update( entry._recid, entry._obj, entry._serializer );
                entry._isDirty = false;
            }
        }
    }
    
    
    /**
     * Obtain an object in the cache
     */
    protected CacheEntry cacheGet(long key) {
        CacheEntry entry = _hash.get(key);
        if (entry != null) 
            touchEntry(entry);
        return entry;        
    }


    /**
     * Place an object in the cache.
     * @throws IOException 
     */
    protected void cachePut(long recid, Object value, Serializer serializer, boolean dirty) throws IOException {
        CacheEntry entry = _hash.get(recid);
        if (entry != null) {
            entry._obj = value;
            touchEntry(entry);
        } else {

            if (_hash.size() == _max) {
                // purge and recycle entry
                entry = purgeEntry();
                entry._recid  = recid;
                entry. _obj = value;
                entry._isDirty = dirty;
            } else {
                entry = new CacheEntry(recid, value, serializer,dirty);
            }
            addEntry(entry);
            _hash.put(entry._recid, entry);
        }
    }

    /**
     * Add a CacheEntry.  Entry goes at the end of the list.
     */
    protected void addEntry(CacheEntry entry) {
        if (_first == null) {
            _first = entry;
            _last = entry;
        } else {
            _last._next = entry;
            entry._previous = _last;
            _last = entry;
        }
    }


    /**
     * Remove a CacheEntry from linked list
     */
    protected void removeEntry(CacheEntry entry) {
        if (entry == _first) {
            _first = entry._next;
        }
        if (_last == entry) {
            _last = entry._previous;
        }
        CacheEntry previous = entry._previous;
        CacheEntry next = entry._next;
        if (previous != null) {
            previous._next = next;
        }
        if (next != null) {
            next._previous = previous;
        }
        entry._previous = null;
        entry._next = null;
    }

    /**
     * Place entry at the end of linked list -- Most Recently Used
     */
    protected void touchEntry(CacheEntry entry) {
        if (_last == entry) {
            return;
        }
        removeEntry(entry);
        addEntry(entry);
    }

    /**
     * Purge least recently used object from the cache
     *
     * @return recyclable CacheEntry
     */
    protected CacheEntry purgeEntry() throws IOException {
        CacheEntry entry = _first;
        if(entry == null)
        	return new CacheEntry(-1,null,null,false);

        if(entry._isDirty)
        	_recman.update( entry._recid, entry._obj, entry._serializer );
        removeEntry(entry);
        _hash.remove(entry._recid);


        entry._obj = null;
        entry._isDirty = false;
        return entry;
    }


    
    protected static final class CacheEntry
    {

        protected long _recid;
        protected Object _obj;
        protected Serializer _serializer;
        protected boolean _isDirty;
        
        protected CacheEntry _previous;
        protected CacheEntry _next;

        
        CacheEntry( long recid, Object obj, Serializer serializer, boolean isDirty )
        {
            _recid = recid;
            _obj = obj;
            _serializer = serializer;
            _isDirty = isDirty;
        }
        
    } 

    protected static final class SoftCacheEntry extends SoftReference
    {

        protected long _recid;
        protected Serializer _serializer;

        
        SoftCacheEntry( long recid, Object obj, Serializer serializer, ReferenceQueue queue)
        {
        	super(obj,queue);
            _recid = recid;
            _serializer = serializer;

        }
        
    }
    
    
    protected static final class SoftRunnable  implements Runnable{

		private ReferenceQueue<SoftCacheEntry> entryQueue;
		private WeakReference<CacheRecordManager> recman2;
		
		public SoftRunnable(CacheRecordManager recman, 
				ReferenceQueue<SoftCacheEntry> entryQueue) {
			this.recman2 = new WeakReference<CacheRecordManager>(recman);
			this.entryQueue = entryQueue;
		}

		public void run() {
			while(true)try{

				//collect next item from cache,
				//limit 500 ms is to keep periodically checking if recman was GCed 
				SoftCacheEntry e = (SoftCacheEntry) entryQueue.remove(500);

				//check if  recman was GCed, cancel in that case
				CacheRecordManager recman = recman2.get();
				if(recman == null) 
					return;
				if(e!=null){
					synchronized(recman._softHash){
						while(e!=null){
							recman._softHash.remove(e._recid);
							e = (SoftCacheEntry) entryQueue.poll();
						}
					}
				}
				
			}catch (InterruptedException e){
				return;
			}catch (Throwable e){
				//this thread must keep spinning, 
				//otherwise SoftCacheEntries would not be disposed
				e.printStackTrace();
			}
		}
    	
    }
  

}
