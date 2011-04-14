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

package jdbm.recman;

import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Iterator;

import jdbm.RecordManager;
import jdbm.Serializer;
import jdbm.helper.LongHashMap;
import jdbm.helper.RecordManagerImpl;

/**
 *  A RecordManager wrapping and caching another RecordManager.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @author <a href="cg@cdegroot.com">Cees de Groot</a>
 * @version $Id: CacheRecordManager.java,v 1.9 2005/06/25 23:12:32 doomdark Exp $
 */
public class CacheRecordManager
    extends RecordManagerImpl
{

    /**
     * Wrapped RecordManager
     */
    protected RecordManager _recman;


    /** Cached object hashtable */
    protected LongHashMap<CacheEntry> _hash;
	
    /** If Soft Cache is enabled, this contains softly referenced clean entries. 
     * If entry became dirty, it is moved to _hash with limited size.
     * This map is accessed from SoftCache Disposer thread, so all access must be 
     * synchronized  
     */
	protected LongHashMap<SoftCacheEntry> _softHash;

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
        _hash = new LongHashMap<CacheEntry>(maxRecords);
        _recman = recman;
        _max = maxRecords;
        _softCache = softCache;
        
        if(softCache){
        	_softHash = new LongHashMap<SoftCacheEntry>();
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

    
    
    public synchronized <A> long insert( A obj, Serializer<A> serializer )
        throws IOException
    {
        checkIfClosed();
        
        long recid = _recman.insert( obj, serializer );
        
        //DONT use cache for inserts, it usually hurts performance on batch inserts
//        if(_softCache) synchronized(_softHash) {
//        	_softHash.put(recid, new SoftCacheEntry(recid, obj, serializer,_refQueue));
//        }else {
//        	cachePut(  recid , obj, serializer, false );
//        }
        return recid;
    }

    public synchronized <A> A fetch( long recid, Serializer<A> serializer, boolean disableCache ) throws IOException{
        if(disableCache)
             return _recman.fetch(recid, serializer,disableCache);
        else
            return fetch(recid,serializer);
     }



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
        	if(e!=null){
        		e.clear();
        	}
        }

    }

    public synchronized <A> void update( long recid, A obj, 
                                     Serializer<A> serializer )
        throws IOException
    {       
        checkIfClosed();
        if(_softCache) synchronized(_softHash) {
        	//soft cache can not contain dirty objects
        	SoftCacheEntry e = _softHash.remove(recid);
        	if(e != null){
        		e.clear();
        	}
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

        
    public synchronized <A> A fetch( long recid, Serializer<A> serializer )
        throws IOException
    {
        checkIfClosed();
        if(_softCache) synchronized(_softHash){
        	SoftCacheEntry e = _softHash.get(recid);
        	if(e!=null){
        		Object a = e.get();
        		if(a!=null){
        			return (A) a;
                }
        	}
        }

        CacheEntry entry = (CacheEntry) cacheGet( recid );
        if ( entry == null ) {
        	A value = _recman.fetch( recid, serializer );
        	if(!_softCache)
        	cachePut(recid,value, serializer,false);
        	else{ //put record into soft cache
        		synchronized(_softHash){
        			_softHash.put(recid,new SoftCacheEntry(recid, value, _refQueue));
        		}
        	}
        	return value;
        }else{
        	return (A) entry._obj;
        }
    }


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



    public synchronized void commit()
        throws IOException
    {
        checkIfClosed();
        updateCacheEntries();
        _recman.commit();
    }

    public synchronized void rollback()
        throws IOException
    {
        checkIfClosed();

        _recman.rollback();

        // discard all cache entries since we don't know which entries
        // where part of the transaction
    	_hash.clear();
    	if(_softCache) synchronized(_softHash) {
        	Iterator<SoftCacheEntry> iter = _softHash.valuesIterator();
        	while(iter.hasNext()){
        		SoftCacheEntry e = iter.next();    		
    			e.clear();
    		}
    		_softHash.clear();
    	}
        _first = null;
        _last = null;
    }


    public synchronized long getNamedObject( String name )
        throws IOException
    {
        checkIfClosed();

        return _recman.getNamedObject( name );
    }


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
    	Iterator<CacheEntry> iter = _hash.valuesIterator();
    	while(iter.hasNext()){
    		CacheEntry entry = iter.next();
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
            entry._serializer = serializer;
            touchEntry(entry);
        } else {

            if (_hash.size() == _max) {
                // purge and recycle entry
                entry = purgeEntry();
                entry._recid  = recid;
                entry. _obj = value;
                entry._isDirty = dirty;
                entry._serializer = serializer;
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
        entry._serializer = null;
        entry._isDirty = false;
        return entry;
    }


    @SuppressWarnings("unchecked")
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

    @SuppressWarnings("unchecked")
	protected static final class SoftCacheEntry extends SoftReference
    {

        protected long _recid;

        SoftCacheEntry( long recid, Object obj,  ReferenceQueue queue)
        {
        	super(obj,queue);
            _recid = recid;
        }
        
    }
    
    
    /**
     * Runs in separate thread and cleans SoftCache. 
     * Runnable auto exists when CacheRecordManager is GCed
     * 
     * @author Jan Kotek
     *
     */
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
				//limit 10000 ms is to keep periodically checking if recman was GCed 
				SoftCacheEntry e = (SoftCacheEntry) entryQueue.remove(10000);

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


	public void clearCache() throws IOException {
        // discard all cache entries since we don't know which entries
        // where part of the transaction
		while(_hash.size()>0)
			purgeEntry();

    	if(_softCache) synchronized(_softHash) {
        	Iterator<SoftCacheEntry> iter = _softHash.valuesIterator();
        	while(iter.hasNext()){
        		SoftCacheEntry e = iter.next();    		
    			e.clear();
    		}
    		_softHash.clear();
    	}
        _first = null;
        _last = null;
		
	}


	public void defrag() throws IOException {
		commit();
		_recman.defrag();		
	}
  

}
