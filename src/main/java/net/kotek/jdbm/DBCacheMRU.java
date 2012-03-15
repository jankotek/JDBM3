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
class DBCacheMRU
        extends DBAbstract {


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
    public DBCacheMRU(DBStore db, int maxRecords) {
        if (db == null) {
            throw new IllegalArgumentException("Argument 'db' is null");
        }
        _hash = new LongHashMap<CacheEntry>(maxRecords);
        _hashDirties = new LongHashMap<CacheEntry>();
        _db = db;
        _max = maxRecords;
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

        cachePut(  recid , obj, serializer, false );


        return recid;
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

        if(_db.needsAutoCommit())
            commit();

    }

    public synchronized <A> void update(final long recid, A obj, Serializer<A> serializer) throws IOException {
        if (_db == null) {
            throw new IllegalStateException("DB has been closed");
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


        //MRU cache is enabled in any case, as it contains modified entries
        CacheEntry entry = cacheGet(recid);
        if (entry != null) {
            return (A) entry._obj;
        }

        //check dirties
        if(_hashDirties.get(recid)!=null){
            System.out.println("Yes");
            return (A) _hashDirties.get(recid)._obj;
        }



        A value = _db.fetch(recid, serializer);

        if(_db.needsAutoCommit())
            commit();


        //put record into MRU cache
        cachePut(recid, value, serializer, false);

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

        _db.rollback();

        // discard all cache entries since we don't know which entries
        // where part of the transaction
        synchronized (_hash){
            _hash.clear();
            _hashDirties.clear();
            _first = null;
            _last = null;
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
                    entry = new CacheEntry(recid, value, serializer);
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
                return new CacheEntry(-1, null, null);

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
