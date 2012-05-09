package org.apache.jdbm;

import javax.crypto.Cipher;
import java.io.IOError;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Abstract class with common cache functionality
 */
abstract class DBCache extends DBStore{

    static final int NUM_OF_DIRTY_RECORDS_BEFORE_AUTOCOMIT = 1024;

    static final byte NONE = 1;
    static final byte MRU = 2;
    static final byte WEAK = 3;
    static final byte SOFT = 4;
    static final byte HARD = 5;

    static final class DirtyCacheEntry {
        long _recid; //TODO recid is already part of _hashDirties, so this field could be removed to save memory
        Object _obj;
        Serializer _serializer;
    }


    /**
     * Dirty status of _hash CacheEntry Values
     */
    final protected LongHashMap<DirtyCacheEntry> _hashDirties = new LongHashMap<DirtyCacheEntry>();

    private Serializer cachedDefaultSerializer = null;


    /**
     * Construct a CacheRecordManager wrapping another DB and
     * using a given cache policy.
     */
    public DBCache(String filename, boolean readonly, boolean transactionDisabled,
                      Cipher cipherIn, Cipher cipherOut, boolean useRandomAccessFile,
                     boolean deleteFilesAfterClose,boolean lockingDisabled){

        super(filename, readonly, transactionDisabled,
                cipherIn, cipherOut, useRandomAccessFile,
                deleteFilesAfterClose,lockingDisabled);

    }


    @Override
    public synchronized  Serializer defaultSerializer(){
        if(cachedDefaultSerializer==null)
            cachedDefaultSerializer = super.defaultSerializer();
        return cachedDefaultSerializer;
    }

    @Override
    boolean needsAutoCommit() {
        return super.needsAutoCommit()||
                (transactionsDisabled &&  !commitInProgress && _hashDirties.size() > NUM_OF_DIRTY_RECORDS_BEFORE_AUTOCOMIT);
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

//        super.update(recid, obj,serializer);

//        return super.insert(obj,serializer,disableCache);

        //and create new dirty record for future update
        final DirtyCacheEntry e = new DirtyCacheEntry();
        e._recid = recid;
        e._obj = obj;
        e._serializer = serializer;
        _hashDirties.put(recid,e);

        return recid;
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

    public synchronized  void rollback(){
        cachedDefaultSerializer = null;
        _hashDirties.clear();
        super.rollback();
    }

    
    private static final Comparator<DirtyCacheEntry> DIRTY_COMPARATOR = new Comparator<DirtyCacheEntry>() {
        final public int compare(DirtyCacheEntry o1, DirtyCacheEntry o2) {
            return (int) (o1._recid - o2._recid);

        }
    };
    

    /**
     * Update all dirty cache objects to the underlying DB.
     */
    protected void updateCacheEntries() {
        try {
            synchronized(_hashDirties){

                while(!_hashDirties.isEmpty()){
                    //make defensive copy of values as _db.update() may trigger changes in db
                    //and this would modify dirties again
                    DirtyCacheEntry[] vals = new DirtyCacheEntry[_hashDirties.size()];
                    Iterator<DirtyCacheEntry> iter = _hashDirties.valuesIterator();

                    for(int i = 0;i<vals.length;i++){
                        vals[i] = iter.next();
                    }
                    iter = null;

                    java.util.Arrays.sort(vals,DIRTY_COMPARATOR);

                    
                    for(int i = 0;i<vals.length;i++){
                        final DirtyCacheEntry entry = vals[i];
                        vals[i] = null;
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



}
