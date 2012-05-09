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

/**
 * A DB wrapping and caching another DB.
 *
 * @author Jan Kotek
 * @author Alex Boisvert
 * @author Cees de Groot
 *
 * TODO add 'cache miss' statistics
 */
class DBCacheMRU
        extends DBCache {


    private static final boolean debug = false;



    /**
     * Cached object hashtable
     */
    protected LongHashMap<CacheEntry> _hash;



    /**
     * Maximum number of objects in the cache.
     */
    protected int _max;



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
     * Construct a CacheRecordManager wrapping another DB and
     * using a given cache policy.
     */
    public DBCacheMRU(String filename, boolean readonly, boolean transactionDisabled,
                      Cipher cipherIn, Cipher cipherOut, boolean useRandomAccessFile,
                     boolean deleteFilesAfterClose, int cacheMaxRecords, boolean lockingDisabled)  {
        super(filename, readonly, transactionDisabled,
            cipherIn, cipherOut, useRandomAccessFile,
            deleteFilesAfterClose,lockingDisabled);

        _hash = new LongHashMap<CacheEntry>(cacheMaxRecords);
        _max = cacheMaxRecords;

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
        synchronized (_hash){
            CacheEntry entry = _hash.get(recid);
            if (entry != null) {
                removeEntry(entry);
                _hash.remove(entry._recid);
            }
            _hashDirties.remove(recid);
        }

        if(super.needsAutoCommit())
            commit();

    }

    public synchronized <A> void update(final long recid, final A obj, final Serializer<A> serializer) throws IOException {
        checkNotClosed();

        synchronized (_hash){

            //remove entry if it already exists
            CacheEntry entry = cacheGet(recid);
            if (entry != null) {
                _hash.remove(recid);
                removeEntry(entry);
            }

            //check if entry is in dirties, in this case just update its object
            DirtyCacheEntry e = _hashDirties.get(recid);
            if(e!=null){
                if(recid!=e._recid) throw new Error();
                e._obj = obj;
                e._serializer = serializer;
                return;
            }
            
            //create new dirty entry
            e = new DirtyCacheEntry();
            e._recid = recid;
            e._obj = obj;
            e._serializer = serializer;
            _hashDirties.put(recid,e);
        }

        if(super.needsAutoCommit())
            commit();

    }


    public synchronized <A> A fetch(long recid, Serializer<A> serializer)
            throws IOException {

        checkNotClosed();

        final CacheEntry entry = cacheGet(recid);
        if (entry != null) {
            return (A) entry._obj;
        }

        //check dirties
        final DirtyCacheEntry entry2 = _hashDirties.get(recid);
        if(entry2!=null){
            return (A) entry2._obj;
        }



        A value = super.fetch(recid, serializer);

        if(super.needsAutoCommit())
            commit();


        //put record into MRU cache
        cachePut(recid, value);

        return value;
    }


    public synchronized void close() {

        if(isClosed())
            return;

        updateCacheEntries();
        super.close();
        _hash = null;
    }



    public synchronized void rollback() {

        // discard all cache entries since we don't know which entries
        // where part of the transaction
        synchronized (_hash){
            _hash.clear();
            _first = null;
            _last = null;
        }

        super.rollback();
    }


    /**
     * Obtain an object in the cache
     */
    protected CacheEntry cacheGet(long key) {
        synchronized (_hash){
            CacheEntry entry = _hash.get(key);
            if ( entry != null && _last != entry) {
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
    protected void cachePut(final long recid, final Object value) throws IOException {
        synchronized (_hash){
            CacheEntry entry = _hash.get(recid);
            if (entry != null) {
                entry._obj = value;
                //touch entry
                if (_last != entry) {
                    removeEntry(entry);
                    addEntry(entry);
                }
            } else {

                if (_hash.size() >= _max) {
                    // purge and recycle entry
                    entry = purgeEntry();
                    entry._recid = recid;
                    entry._obj = value;
                } else {
                    entry = new CacheEntry(recid, value);
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
                return new CacheEntry(-1, null);

            removeEntry(entry);
            _hash.remove(entry._recid);
            entry._obj = null;
            return entry;
        }
    }




    @SuppressWarnings("unchecked")
    static final class CacheEntry {

        protected long _recid;
        protected Object _obj;


        protected CacheEntry _previous;
        protected CacheEntry _next;


        CacheEntry(long recid, Object obj) {
            _recid = recid;
            _obj = obj;
        }

    }


    public void clearCache() {
        if(debug)
            System.err.println("DBCache: Clear cache");

        // discard all cache entries since we don't know which entries
        // where part of the transaction
        synchronized (_hash){
            _hash.clear();
            _first = null;
            _last = null;

            //clear dirties
            updateCacheEntries();

        }
    }


}
