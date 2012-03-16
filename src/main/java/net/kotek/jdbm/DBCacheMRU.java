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

import javax.crypto.Cipher;
import java.io.IOError;
import java.io.IOException;
import java.util.Iterator;

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
        extends DBStore {


    private static final boolean debug = false;



    /**
     * Cached object hashtable
     */
    protected LongHashMap<CacheEntry> _hash;

    /**
     * Dirty status of _hash CacheEntry Values
     */
    protected LongHashMap<CacheEntry> _hashDirties;




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
                      boolean autodefrag,boolean deleteFilesAfterClose, int cacheMaxRecords)  {
        super(filename, readonly, transactionDisabled,
            cipherIn, cipherOut, useRandomAccessFile,
            autodefrag,deleteFilesAfterClose);

        _hash = new LongHashMap<CacheEntry>(cacheMaxRecords);
        _hashDirties = new LongHashMap<CacheEntry>();

        _max = cacheMaxRecords;

    }


    public synchronized <A> long insert(final A obj, final Serializer<A> serializer, final boolean disableCache)
            throws IOException {
        checkNotClosed();

        if(super.needsAutoCommit())
            commit();

        if(disableCache)
            return super.insert(obj, serializer, disableCache);


        //prealocate recid so we have something to return
        final long recid = super.insert(PREALOCATE_OBJ, null, disableCache);

        //and create new dirty record for future update
        cachePut(  recid , obj, serializer, true );

        return recid;
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
                _hashDirties.remove(entry._recid);
            }
        }

        if(super.needsAutoCommit())
            commit();

    }

    public synchronized <A> void update(final long recid, A obj, Serializer<A> serializer) throws IOException {
        checkNotClosed();

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

        if(super.needsAutoCommit())
            commit();

    }


    public synchronized <A> A fetch(long recid, Serializer<A> serializer)
            throws IOException {

        checkNotClosed();

        //MRU cache is enabled in any case, as it contains modified entries
        CacheEntry entry = cacheGet(recid);
        if (entry != null) {
            return (A) entry._obj;
        }

        //check dirties
        if(_hashDirties.get(recid)!=null){
            return (A) _hashDirties.get(recid)._obj;
        }



        A value = super.fetch(recid, serializer);

        if(super.needsAutoCommit())
            commit();


        //put record into MRU cache
        cachePut(recid, value, serializer, false);

        return value;
    }


    public synchronized void close() {

        if(isClosed())
            return;

        updateCacheEntries();
        super.close();
        _hash = null;
        _hashDirties = null;
    }



    public synchronized void commit() {
        try{
            commitInProgress = true;
            updateCacheEntries();
            super.commit();
        }finally {
            commitInProgress = false;
        }
    }

    public synchronized void rollback() {

        // discard all cache entries since we don't know which entries
        // where part of the transaction
        synchronized (_hash){
            _hash.clear();
            _hashDirties.clear();
            _first = null;
            _last = null;
        }

        super.rollback();
    }






    /**
     * Update all dirty cache objects to the underlying DB.
     */
    protected void updateCacheEntries() {
        try {
            synchronized(_hash){

                while(!_hashDirties.isEmpty()){
                    //make defensive copy of values as _db.update() may trigger changes in db
                    CacheEntry[] vals = new CacheEntry[_hashDirties.size()];
                    Iterator<CacheEntry> iter = _hashDirties.valuesIterator();

                    for(int i = 0;i<vals.length;i++){
                        vals[i] = iter.next();
                    }
                    iter = null;

                    
                    for(CacheEntry entry:vals){
                        super.update(entry._recid, entry._obj, entry._serializer);
                        _hashDirties.remove(entry._recid);
                    }


                    //update may have triggered more records to be added into dirties, so repeat until all records are written.
                }
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
    protected void cachePut(final long recid, final Object value,
                            final Serializer serializer, final boolean dirty) throws IOException {
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
                    entry = new CacheEntry(recid, value, serializer);
                }
                addEntry(entry);
                _hash.put(entry._recid, entry);
            }
            if(dirty){
                //set dirty if needed
                setCacheEntryDirty(entry,true);
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
                return new CacheEntry(-1, null, null);



            removeEntry(entry);
            _hash.remove(entry._recid);
            if (isCacheEntryDirty(entry))
                return new CacheEntry(-1, null, null);
                //is already in dirty cache, can not reuse node

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


        CacheEntry(long recid, Object obj, Serializer serializer) {
            _recid = recid;
            _obj = obj;
            _serializer = serializer;
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

            //clear dirties
            updateCacheEntries();

        }


    }





}
