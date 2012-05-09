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

import javax.crypto.Cipher;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.Iterator;
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
        extends DBCache {



    private static final boolean debug = false;



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
    public DBCacheRef(String filename, boolean readonly, boolean transactionDisabled,
                      Cipher cipherIn, Cipher cipherOut, boolean useRandomAccessFile,
                      boolean deleteFilesAfterClose,
                      byte cacheType, boolean cacheAutoClearOnLowMem, boolean lockingDisabled) {

        super(filename, readonly, transactionDisabled,
                cipherIn, cipherOut, useRandomAccessFile,
                deleteFilesAfterClose, lockingDisabled);


        this._cacheType = cacheType;
        _autoClearReferenceCacheOnLowMem = cacheAutoClearOnLowMem;


        _softHash = new LongHashMap<ReferenceCacheEntry>();
        _refQueue = new ReferenceQueue<ReferenceCacheEntry>();
        _softRefThread = new Thread(
                    new SoftRunnable(this, _refQueue),
                    "JDBM Soft Cache Disposer " + (threadCounter.incrementAndGet()));
        _softRefThread.setDaemon(true);
        _softRefThread.start();

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
            return super.fetch(recid, serializer, disableCache);
        else
            return fetch(recid, serializer);
    }


    public synchronized void delete(long recid)
            throws IOException {
        checkNotClosed();

        super.delete(recid);
        synchronized (_hashDirties){
            _hashDirties.remove(recid);
        }
        synchronized (_softHash) {
            Object e = _softHash.remove(recid);
            if (e != null && e instanceof ReferenceCacheEntry) {
                ((ReferenceCacheEntry)e).clear();
            }
        }

        if(needsAutoCommit())
            commit();

    }

    public synchronized <A> void update(final long recid, A obj, Serializer<A> serializer) throws IOException {
        checkNotClosed();


        synchronized (_softHash) {
            //soft cache can not contain dirty objects
            Object e = _softHash.remove(recid);
            if (e != null && e instanceof ReferenceCacheEntry) {
                ((ReferenceCacheEntry)e).clear();
            }
        }
        synchronized (_hashDirties){
            //put into dirty cache
            final DirtyCacheEntry e = new DirtyCacheEntry();
            e._recid = recid;
            e._obj = obj;
            e._serializer = serializer;
            _hashDirties.put(recid,e);
        }

        if(needsAutoCommit())
            commit();

    }


    public synchronized <A> A fetch(long recid, Serializer<A> serializer)
            throws IOException {
        checkNotClosed();

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
            DirtyCacheEntry e2 = _hashDirties.get(recid);
            if(e2!=null){
                return (A) e2._obj;
            }
        }




        A value = super.fetch(recid, serializer);

        if(needsAutoCommit())
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
        checkNotClosed();

        updateCacheEntries();
        super.close();
        _softHash = null;
        _softRefThread.interrupt();
    }




    public synchronized void rollback() {
        checkNotClosed();


        // discard all cache entries since we don't know which entries
        // where part of the transaction
        synchronized (_softHash) {
            Iterator<ReferenceCacheEntry> iter = _softHash.valuesIterator();
            while (iter.hasNext()) {
                ReferenceCacheEntry e = iter.next();
                e.clear();
            }
            _softHash.clear();
        }

        super.rollback();
    }




    
	protected boolean isCacheEntryDirty(DirtyCacheEntry entry) {
		return _hashDirties.get(entry._recid) != null;
	}

	protected void setCacheEntryDirty(DirtyCacheEntry entry, boolean dirty) {
		if (dirty) {
			_hashDirties.put(entry._recid, entry);
		} else {
			_hashDirties.remove(entry._recid);
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



}
