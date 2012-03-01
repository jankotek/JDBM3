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
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * A DB wrapping and caching another DB.
 *
 * @author Jan Kotek
 * @author Alex Boisvert
 * @author Cees de Groot
 *
 * TODO add 'cache miss' statistics
 */
class DBCache
        extends DBAbstract {


    static final byte NONE = 1;
    static final byte MRU = 2;
    static final byte WEAK = 3;
    static final byte SOFT = 4;
    static final byte HARD = 5;

    private static final boolean debug = false;

    /**
     * Wrapped DB
     */
    protected DBStore _db;


    /**
     * Cached object hashtable
     */
    protected LongHashMap<CacheEntry> _hash;
    
    /**
     * Dirty status of _hash CacheEntry Values
     */
    protected LongHashMap<CacheEntry> _hashDirties;

    /**
     * If Soft Cache is enabled, this contains softly referenced clean entries.
     * If entry became dirty, it is moved to _hash with limited size.
     * This map is accessed from SoftCache Disposer thread, so all access must be
     * synchronized
     */
    protected LongHashMap _softHash;

    /**
     * Reference queue used to collect Soft Cache entries
     */
    protected ReferenceQueue<ReferenceCacheEntry> _refQueue;


    /**
     * Maximum number of objects in the cache.
     */
    protected int _max;


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

    /** counter which counts number of insert since last 'action'*/
    protected int insertCounter = 0;

    private boolean _autoClearReferenceCacheOnLowMem;
    private byte _cacheType;


    /**
     * Construct a CacheRecordManager wrapping another DB and
     * using a given cache policy.
     */
    public DBCache(DBStore db, int maxRecords,
                   byte cacheType,
                   boolean autoClearReferenceCacheOnLowMem) {
        if (db == null) {
            throw new IllegalArgumentException("Argument 'db' is null");
        }
        _hash = new LongHashMap<CacheEntry>(maxRecords);
        _hashDirties = new LongHashMap<CacheEntry>();
        _db = db;
        _max = maxRecords;
        this._cacheType = cacheType;
        _autoClearReferenceCacheOnLowMem = autoClearReferenceCacheOnLowMem;

        if (cacheType>MRU) {
            _softHash = new LongHashMap<ReferenceCacheEntry>();
            _refQueue = new ReferenceQueue<ReferenceCacheEntry>();
            _softRefThread = new Thread(
                    new SoftRunnable(this, _refQueue),
                    "JDBM Soft Cache Disposer " + (threadCounter++));
            _softRefThread.setDaemon(true);
            _softRefThread.start();
        }
        db.wrappedInCache = true;

    }


    public synchronized <A> long insert(final A obj, final Serializer<A> serializer, final boolean disableCache)
            throws IOException {
        if (_db == null) {
            throw new IllegalStateException("DB has been closed");
        }

        if(_db.needsAutoCommit())
            commit();

        final long recid = _db.insert(obj, serializer,disableCache);


        if(disableCache) return recid;

        if(_cacheType>MRU) synchronized(_softHash) {

            if (_cacheType == SOFT)
                _softHash.put(recid, new SoftCacheEntry(recid, obj, _refQueue));
            else if (_cacheType == WEAK)
                _softHash.put(recid, new WeakCacheEntry(recid, obj, _refQueue));
            else
                _softHash.put(recid,obj);
        }else {
            cachePut(  recid , obj, serializer, false );
        }

        return recid;
    }

    void clearCacheIfLowOnMem() {

        insertCounter = 0;

        if(!_autoClearReferenceCacheOnLowMem)
            return;

        Runtime r = Runtime.getRuntime();
        long max = r.maxMemory();
        if(max == Long.MAX_VALUE)
            return;

        double free = r.freeMemory();
        double total = r.totalMemory();
        //We believe that free refers to total not max.
        //Increasing heap size to max would increase to max
        free = free + (max-total);

        if(debug)
            System.err.println("DBCache: freemem = " +free + " = "+(free/max)+"%");

        if(free<1e7 || free*4 <max)
            clearCache();


    }

    public synchronized <A> A fetch(long recid, Serializer<A> serializer, boolean disableCache) throws IOException {

        if (disableCache)
            return _db.fetch(recid, serializer, disableCache);
        else
            return fetch(recid, serializer);
    }


    public synchronized void delete(long recid)
            throws IOException {
        if (_db == null) {
            throw new IllegalStateException("DB has been closed");
        }

        _db.delete(recid);
        synchronized (_hash){
            CacheEntry entry = _hash.get(recid);
            if (entry != null) {
                removeEntry(entry);
                _hash.remove(entry._recid);
                _hashDirties.remove(entry._recid);
            }
        }
        if (_cacheType>MRU) synchronized (_softHash) {
            Object e = _softHash.remove(recid);
            if (e != null && e instanceof ReferenceCacheEntry) {
                ((ReferenceCacheEntry)e).clear();
            }
        }

        if(_db.needsAutoCommit())
            commit();

    }

    public synchronized <A> void update(final long recid, A obj, Serializer<A> serializer) throws IOException {
        if (_db == null) {
            throw new IllegalStateException("DB has been closed");
        }


        if (_cacheType>MRU) synchronized (_softHash) {
            //soft cache can not contain dirty objects
            Object e = _softHash.remove(recid);
            if (e != null && e instanceof ReferenceCacheEntry) {
                ((ReferenceCacheEntry)e).clear();
            }
        }
        synchronized (_hash){

            CacheEntry entry = cacheGet(recid);
            if (entry != null) {
                // reuse existing cache entry
                entry._obj = obj;
                entry._serializer = serializer;
                setCacheEntryDirty(entry, true);
            } else {
                cachePut(recid, obj, serializer, true);
            }
        }

        if(_db.needsAutoCommit())
            commit();

    }


    public synchronized <A> A fetch(long recid, Serializer<A> serializer)
            throws IOException {
        if (_db == null) {
            throw new IllegalStateException("DB has been closed");
        }

        if (_cacheType>MRU) synchronized (_softHash) {
            Object e = _softHash.get(recid);
            if (e != null) {

                if(e instanceof ReferenceCacheEntry)
                    e = ((ReferenceCacheEntry)e).get();
                if (e != null) {
                    return (A) e;
                }
            }
        }

        //MRU cache is enabled in any case, as it contains modified entries
        CacheEntry entry = cacheGet(recid);
        if (entry != null) {
            return (A) entry._obj;
        }



        A value = _db.fetch(recid, serializer);

        if(_db.needsAutoCommit())
            commit();

        if (_cacheType==MRU){
            //put record into MRU cache
            cachePut(recid, value, serializer, false);
        }else { //put record into soft cache
           synchronized (_softHash) {

                    if (_cacheType == SOFT)
                        _softHash.put(recid, new SoftCacheEntry(recid, value, _refQueue));
                    else if (_cacheType == WEAK)
                        _softHash.put(recid, new WeakCacheEntry(recid, value, _refQueue));
                    else
                        _softHash.put(recid,value);
           }
        }

        return value;
    }


    public synchronized void close() {
        if (_db == null) {
            throw new IllegalStateException("DB has been closed");
        }

        updateCacheEntries();
        _db.close();
        _db = null;
        _hash = null;
        _hashDirties = null;
        _softHash = null;
        if (_cacheType>MRU)
            _softRefThread.interrupt();
    }


    public synchronized void commit() {
        if (_db == null) {
            throw new IllegalStateException("DB has been closed");
        }

        updateCacheEntries();

        _db.commit();
    }

    public synchronized void rollback() {
        if (_db == null) {
            throw new IllegalStateException("DB has been closed");
        }

        _db.rollback();

        // discard all cache entries since we don't know which entries
        // where part of the transaction
        synchronized (_hash){
            _hash.clear();
            _hashDirties.clear();
            _first = null;
            _last = null;
        }
        if (_cacheType>MRU) synchronized (_softHash) {
            Iterator<ReferenceCacheEntry> iter = _softHash.valuesIterator();
            while (iter.hasNext()) {
                ReferenceCacheEntry e = iter.next();
                e.clear();
            }
            _softHash.clear();
        }
    }


    public synchronized long getNamedObject(String name)
            throws IOException {
        if (_db == null) {
            throw new IllegalStateException("DB has been closed");
        }

        return _db.getNamedObject(name);
    }


    public synchronized void setNamedObject(String name, long recid)
            throws IOException {
        if (_db == null) {
            throw new IllegalStateException("DB has been closed");
        }

        _db.setNamedObject(name, recid);
    }

    public Serializer defaultSerializer() {
        return _db.defaultSerializer();
    }

    public String calculateStatistics() {
        if (_db == null) {
            throw new IllegalStateException("DB has been closed");
        }

        return _db.calculateStatistics();
    }


    /**
     * Update all dirty cache objects to the underlying DB.
     */
    protected void updateCacheEntries() {
        try {
            synchronized(_hash){

                //make defensive copy of values as _db.update() may trigger changes in db

                CacheEntry[] vals = new CacheEntry[_hashDirties.size()];
                Iterator<CacheEntry> iter = _hashDirties.valuesIterator();
                
                for(int i = 0;i<vals.length;i++){
                    vals[i] = iter.next();
                }
                iter = null;

                for(CacheEntry entry:vals){
                    _db.update(entry._recid, entry._obj, entry._serializer);
                }                
                _hashDirties.clear();
                //TODO entries are not dirty anymore, maybe _hash.clear()?
            }
        } catch (IOException e) {
            throw new IOError(e);
        }

    }


    /**
     * Obtain an object in the cache
     */
    protected CacheEntry cacheGet(long key) {
        synchronized (_hash){
            CacheEntry entry = _hash.get(key);
            if (!(_cacheType>MRU) && entry != null && _last != entry) {
                //touch entry
                removeEntry(entry);
                addEntry(entry);
            }
            return entry;
        }
    }


    /**
     * Place an object in the cache.
     *
     * @throws IOException
     */
    protected void cachePut(long recid, Object value, Serializer serializer, boolean dirty) throws IOException {
        synchronized (_hash){
            CacheEntry entry = _hash.get(recid);
            if (entry != null) {
                entry._obj = value;
                entry._serializer = serializer;
                //touch entry
                if (_last != entry) {
                    removeEntry(entry);
                    addEntry(entry);
                }
            } else {

                if (_hash.size() == _max) {
                    // purge and recycle entry
                    entry = purgeEntry();
                    entry._recid = recid;
                    entry._obj = value;
                    setCacheEntryDirty(entry, dirty);
                    entry._serializer = serializer;
                } else {
                    entry = new CacheEntry(recid, value, serializer, dirty);
                }
                addEntry(entry);
                _hash.put(entry._recid, entry);
            }
        }
    }

    /**
     * Add a CacheEntry.  Entry goes at the end of the list.
     */
    protected void addEntry(CacheEntry entry) {
        synchronized (_hash){
            if (_first == null) {
                _first = entry;
                _last = entry;
            } else {
                _last._next = entry;
                entry._previous = _last;
                _last = entry;
            }
        }
    }


    /**
     * Remove a CacheEntry from linked list
     */
    protected void removeEntry(CacheEntry entry) {
        synchronized (_hash){
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
    }

    /**
     * Purge least recently used object from the cache
     *
     * @return recyclable CacheEntry
     */
    protected CacheEntry purgeEntry() {
        synchronized (_hash){
            CacheEntry entry = _first;
            if (entry == null)
                return new CacheEntry(-1, null, null, false);

            if (isCacheEntryDirty(entry)) try {
                _db.update(entry._recid, entry._obj, entry._serializer);
            } catch (IOException e) {
                throw new IOError(e);
            }


            removeEntry(entry);
            _hash.remove(entry._recid);
            entry._obj = null;
            entry._serializer = null;
            setCacheEntryDirty(entry, false);
            return entry;
        }
    }
    
	protected boolean isCacheEntryDirty(CacheEntry entry) {
		return _hashDirties.get(entry._recid) != null;
	}

	protected void setCacheEntryDirty(CacheEntry entry, boolean dirty) {
		if (dirty) {
			_hashDirties.put(entry._recid, entry);
		} else {
			_hashDirties.remove(entry._recid);
		}
	}


    @SuppressWarnings("unchecked")
    static final class CacheEntry {

        protected long _recid;
        protected Object _obj;

        protected Serializer _serializer;

        protected CacheEntry _previous;
        protected CacheEntry _next;


        CacheEntry(long recid, Object obj, Serializer serializer, boolean isDirty) {
            _recid = recid;
            _obj = obj;
            _serializer = serializer;
        }

    }

    interface ReferenceCacheEntry {
        long getRecid();

        void clear();

        Object get();
    }

    @SuppressWarnings("unchecked")
    static final class SoftCacheEntry extends SoftReference implements ReferenceCacheEntry {
        protected final long _recid;

        public long getRecid() {
            return _recid;
        }

        SoftCacheEntry(long recid, Object obj, ReferenceQueue queue) {
            super(obj, queue);
            _recid = recid;
        }
    }

    @SuppressWarnings("unchecked")
    static final class WeakCacheEntry extends WeakReference implements ReferenceCacheEntry {
        protected final long _recid;

        public long getRecid() {
            return _recid;
        }

        WeakCacheEntry(long recid, Object obj, ReferenceQueue queue) {
            super(obj, queue);
            _recid = recid;
        }


    }


    /**
     * Runs in separate thread and cleans SoftCache.
     * Runnable auto exists when CacheRecordManager is GCed
     *
     * @author Jan Kotek
     */
    static final class SoftRunnable implements Runnable {

        private ReferenceQueue<ReferenceCacheEntry> entryQueue;
        private WeakReference<DBCache> db2;

        public SoftRunnable(DBCache db,
                            ReferenceQueue<ReferenceCacheEntry> entryQueue) {
            this.db2 = new WeakReference<DBCache>(db);
            this.entryQueue = entryQueue;
        }

        public void run() {
            while (true) try {

                //collect next item from cache,
                //limit 10000 ms is to keep periodically checking if db was GCed
                ReferenceCacheEntry e = (ReferenceCacheEntry) entryQueue.remove(10000);

                //check if  db was GCed, cancel in that case
                DBCache db = db2.get();
                if (db == null)
                    return;

                if (e != null) {

                    synchronized (db._softHash) {
                        int counter = 0;
                        while (e != null) {
                            db._softHash.remove(e.getRecid());
                            e = (SoftCacheEntry) entryQueue.poll();
                            if(debug)
                                counter++;
                        }
                        if(debug)
                            System.err.println("DBCache: "+counter+" objects released from ref cache.");
                    }
                }else{
                    //check memory consumption every 10 seconds
                    db.clearCacheIfLowOnMem();

                }


            } catch (InterruptedException e) {
                return;
            } catch (Throwable e) {
                //this thread must keep spinning,
                //otherwise SoftCacheEntries would not be disposed
                e.printStackTrace();
            }
        }

    }


    public void clearCache() {
        if(debug)
            System.err.println("DBCache: Clear cache");

        // discard all cache entries since we don't know which entries
        // where part of the transaction
        synchronized (_hash){
            while (_hash.size() > 0){
                purgeEntry();
            }
            _first = null;
            _last = null;
        }

        if (_cacheType>MRU) synchronized (_softHash) {
            if(_cacheType!=HARD){
                Iterator<ReferenceCacheEntry> iter = _softHash.valuesIterator();
                while (iter.hasNext()) {
                    ReferenceCacheEntry e = iter.next();
                    e.clear();
                }
            }
            _softHash.clear();
        }

    }


    public void defrag(boolean sortCollections) {
        commit();
        _db.defrag(sortCollections);
    }



    public Map<String,Object> getCollections(){
        return  _db.getCollections();
    }

    /** completely remove collection from store*/
    public void deleteCollection(String name){
        _db.deleteCollection(name);
    }


}
