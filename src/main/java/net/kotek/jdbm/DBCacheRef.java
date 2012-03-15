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
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A DB wrapping and caching another DB.
 *
 * @author Jan Kotek
 * @author Alex Boisvert
 * @author Cees de Groot
 *
 * TODO add 'cache miss' statistics
 */
public class DBCacheRef
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

    protected static AtomicInteger threadCounter = new AtomicInteger(0);


    /** counter which counts number of insert since last 'action'*/
    protected int insertCounter = 0;

    private final boolean _autoClearReferenceCacheOnLowMem;
    private final byte _cacheType;


    /**
     * Construct a CacheRecordManager wrapping another DB and
     * using a given cache policy.
     */
    public DBCacheRef(DBStore db, int maxRecords,
                   byte cacheType,
                   boolean autoClearReferenceCacheOnLowMem) {
        if (db == null) {
            throw new IllegalArgumentException("Argument 'db' is null");
        }
        _hashDirties = new LongHashMap<CacheEntry>();
        _db = db;
        _max = maxRecords;
        this._cacheType = cacheType;
        _autoClearReferenceCacheOnLowMem = autoClearReferenceCacheOnLowMem;


        _softHash = new LongHashMap<ReferenceCacheEntry>();
        _refQueue = new ReferenceQueue<ReferenceCacheEntry>();
        _softRefThread = new Thread(
                    new SoftRunnable(this, _refQueue),
                    "JDBM Soft Cache Disposer " + (threadCounter.incrementAndGet()));
        _softRefThread.setDaemon(true);
        _softRefThread.start();

        db.wrappedInCache = true;

    }


    public synchronized <A> long insert(final A obj, final Serializer<A> serializer, final boolean disableCache)
            throws IOException {
        if (_db == null) {
            throw new IllegalStateException("DB has been closed");
        }

        if(_db.needsAutoCommit())
            commit();

        final long recid = _db.insert(obj, serializer, disableCache);


        if(disableCache) return recid;

        synchronized(_softHash) {

            if (_cacheType == SOFT)
                _softHash.put(recid, new SoftCacheEntry(recid, obj, _refQueue));
            else if (_cacheType == WEAK)
                _softHash.put(recid, new WeakCacheEntry(recid, obj, _refQueue));
            else
                _softHash.put(recid,obj);
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
        synchronized (_hashDirties){
            _hashDirties.remove(recid);
        }
        synchronized (_softHash) {
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


        synchronized (_softHash) {
            //soft cache can not contain dirty objects
            Object e = _softHash.remove(recid);
            if (e != null && e instanceof ReferenceCacheEntry) {
                ((ReferenceCacheEntry)e).clear();
            }
        }
        synchronized (_hashDirties){
            //put into dirty cache
            final CacheEntry e = new CacheEntry();
            e._recid = recid;
            e._obj = obj;
            e._serializer = serializer;
            _hashDirties.put(recid,e);
        }

        if(_db.needsAutoCommit())
            commit();

    }


    public synchronized <A> A fetch(long recid, Serializer<A> serializer)
            throws IOException {
        if (_db == null) {
            throw new IllegalStateException("DB has been closed");
        }

        synchronized (_softHash) {
            Object e = _softHash.get(recid);
            if (e != null) {

                if(e instanceof ReferenceCacheEntry)
                    e = ((ReferenceCacheEntry)e).get();
                if (e != null) {
                    return (A) e;
                }
            }
        }


        synchronized (_hashDirties){
            CacheEntry e2 = _hashDirties.get(recid);
            if(e2!=null){
                return (A) e2._obj;
            }
        }




        A value = _db.fetch(recid, serializer);

        if(_db.needsAutoCommit())
            commit();

           synchronized (_softHash) {

                    if (_cacheType == SOFT)
                        _softHash.put(recid, new SoftCacheEntry(recid, value, _refQueue));
                    else if (_cacheType == WEAK)
                        _softHash.put(recid, new WeakCacheEntry(recid, value, _refQueue));
                    else
                        _softHash.put(recid,value);
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
        _hashDirties = null;
        _softHash = null;
        _softRefThread.interrupt();
    }

    public synchronized boolean isClosed(){
        return _db == null;
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



        // discard all cache entries since we don't know which entries
        // where part of the transaction
        synchronized (_hashDirties){
            _hashDirties.clear();
        }
        synchronized (_softHash) {
            Iterator<ReferenceCacheEntry> iter = _softHash.valuesIterator();
            while (iter.hasNext()) {
                ReferenceCacheEntry e = iter.next();
                e.clear();
            }
            _softHash.clear();
        }

        _db.rollback();
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
            synchronized(_hashDirties){

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
            }
        } catch (IOException e) {
            throw new IOError(e);
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
    private static final class CacheEntry {

        protected long _recid;
        protected Object _obj;

        protected Serializer _serializer;


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
        private WeakReference<DBCacheRef> db2;

        public SoftRunnable(DBCacheRef db,
                            ReferenceQueue<ReferenceCacheEntry> entryQueue) {
            this.db2 = new WeakReference<DBCacheRef>(db);
            this.entryQueue = entryQueue;
        }

        public void run() {
            while (true) try {

                //collect next item from cache,
                //limit 10000 ms is to keep periodically checking if db was GCed
                ReferenceCacheEntry e = (ReferenceCacheEntry) entryQueue.remove(10000);

                //check if  db was GCed, cancel in that case
                DBCacheRef db = db2.get();
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


        synchronized (_softHash) {
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
