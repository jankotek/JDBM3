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
import java.io.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * This class manages records, which are uninterpreted blobs of data. The
 * set of operations is simple and straightforward: you communicate with
 * the class using long "rowids" and byte[] data blocks. Rowids are returned
 * on inserts and you can stash them away someplace safe to be able to get
 * back to them. Data blocks can be as long as you wish, and may have
 * lengths different from the original when updating.
 * <p/>
 * Operations are synchronized, so that only one of them will happen
 * concurrently even if you hammer away from multiple threads. Operations
 * are made atomic by keeping a transaction log which is recovered after
 * a crash, so the operations specified by this interface all have ACID
 * properties.
 * <p/>
 * You identify a file by just the name. The package attaches <tt>.db</tt>
 * for the database file, and <tt>.lg</tt> for the transaction log. The
 * transaction log is synchronized regularly and then restarted, so don't
 * worry if you see the size going up and down.
 *
 * @author Alex Boisvert
 * @author Cees de Groot
 */
final class DBStore
        extends DBAbstract {


    /**
     * Version of storage. It should be safe to open lower versions, but engine should throw exception
     * while opening new versions (as it contains unsupported features or serialization)
     */
    static final long STORE_FORMAT_VERSION = 1L;
    
    RecordManager recman;


    /**
     * Indicated that store is opened for readonly operations
     * If true, store will throw UnsupportedOperationException when update/insert/delete operation is called
     */
    private final boolean readonly;
    private final boolean transactionsDisabled;
    private final boolean autodefrag;




    /**
     * cipher used for decryption, may be null
     */
    private Cipher cipherOut;
    /**
     * cipher used for encryption, may be null
     */
    private Cipher cipherIn;
    private boolean useRandomAccessFile;

    /** If this DB is wrapped in DBCache, it is not responsible to drive the auto commits*/
    boolean wrappedInCache = false;
    
    final private boolean deleteFilesAfterClose;


    void checkCanWrite() {
        if (readonly)
            throw new UnsupportedOperationException("Could not write, store is opened as read-only");
    }






    /**
     * Static debugging flag
     */
    public static final boolean DEBUG = false;


    /**
     * Directory of named JDBMHashtables.  This directory is a persistent
     * directory, stored as a Hashtable.  It can be retrived by using
     * the NAME_DIRECTORY_ROOT.
     */
    private Map<String, Long> _nameDirectory;

    /**
     * Reserved slot for name directory recid.
     */
    public static final int NAME_DIRECTORY_ROOT = 0;


    /**
     * Reserved slot for version number
     */
    public static final int STORE_VERSION_NUMBER_ROOT = 1;

    /**
     * Reserved slot for recid where Serial class info is stored
     */
    public static final int SERIAL_CLASS_INFO_RECID_ROOT = 2;


    private final DataInputOutput buffer = new DataInputOutput();
    private boolean bufferInUse = false;


    private final String _filename;

    public DBStore(String filename, boolean readonly, boolean transactionDisabled) throws IOException {
        this(filename, readonly, transactionDisabled, null, null, false,true,false);
    }


    /**
     * Creates a record manager for the indicated file
     *
     * @throws IOException when the file cannot be opened or is not
     *                     a valid file content-wise.
     */
    public DBStore(String filename, boolean readonly, boolean transactionDisabled,
                   Cipher cipherIn, Cipher cipherOut, boolean useRandomAccessFile,
                   boolean autodefrag, boolean deleteFilesAfterClose)
            throws IOException {
        _filename = filename;
        this.readonly = readonly;
        this.transactionsDisabled = transactionDisabled;
        this.cipherIn = cipherIn;
        this.cipherOut = cipherOut;
        this.useRandomAccessFile = useRandomAccessFile;
        this.autodefrag = autodefrag;
        this.deleteFilesAfterClose = deleteFilesAfterClose;
        reopen();
    }


    private void reopen() throws IOException {
        recman = new RecordManagerNative(_filename, readonly, transactionsDisabled, cipherIn, cipherOut,useRandomAccessFile,autodefrag);

        long versionNumber = recman.getRoot(STORE_VERSION_NUMBER_ROOT);
        if (versionNumber > STORE_FORMAT_VERSION)
            throw new IOException("Unsupported version of store. Please update JDBM. Minimal supported ver:" + STORE_FORMAT_VERSION + ", store ver:" + versionNumber);
        if (!readonly)
            recman.setRoot(STORE_VERSION_NUMBER_ROOT, STORE_FORMAT_VERSION);


        defaultSerializer = null;

    }


    public synchronized void close() {
        checkIfClosed();
        try {
            recman.close();
            if(deleteFilesAfterClose)
                recman.deleteAllFiles();
            recman = null;

        } catch (IOException e) {
            throw new IOError(e);
        }
    }
    
    public synchronized boolean isClosed(){
        return recman == null;
    }


    public synchronized <A> long insert(final A obj, final Serializer<A> serializer, final boolean disableCache)
            throws IOException {
        checkIfClosed();
        checkCanWrite();

        if (!wrappedInCache && needsAutoCommit()) {
            commit();
        }


        if (bufferInUse) {
            //current reusable buffer is in use, have to fallback into creating new instances
            DataInputOutput buffer2 = new DataInputOutput();
            return insert2(obj, serializer, buffer2);
        }

        try {

            bufferInUse = true;
            return insert2(obj, serializer, buffer);
        } finally {
            bufferInUse = false;
        }


    }

    boolean needsAutoCommit() {
        return  recman.needsAutoCommit();
    }


    private <A> long insert2(A obj, Serializer<A> serializer, DataInputOutput buf)
            throws IOException {
        buf.reset();

        serializer.serialize(buf, obj);
        return recman.insert(buf.getBuf(), 0, buf.getPos());
    }


    public synchronized void delete(long logRowId)
            throws IOException {

        checkIfClosed();
        checkCanWrite();
        if (logRowId <= 0) {
            throw new IllegalArgumentException("Argument 'recid' is invalid: "
                    + logRowId);
        }

        recman.delete(logRowId);

        if (!wrappedInCache && needsAutoCommit()) {
            commit();
        }


    }


    public synchronized <A> void update(long recid, A obj, Serializer<A> serializer)
            throws IOException {
        checkIfClosed();
        checkCanWrite();
        if (recid <= 0) {
            throw new IllegalArgumentException("Argument 'recid' is invalid: "
                    + recid);
        }

        if (!wrappedInCache && needsAutoCommit()) {
            commit();
        }

        if (bufferInUse) {
            //current reusable buffer is in use, have to create new instances
            DataInputOutput buffer2 = new DataInputOutput();
            update2(recid, obj, serializer, buffer2);
            return;
        }

        try {
            bufferInUse = true;
            update2(recid, obj, serializer, buffer);
        } finally {
            bufferInUse = false;
        }
    }


    private <A> void update2(long logRecid, final A obj, final Serializer<A> serializer, final DataInputOutput buf)
            throws IOException {

        buf.reset();
        serializer.serialize(buf, obj);

        recman.update(logRecid, buf.getBuf(),0,buf.getPos());

    }


    public synchronized <A> A fetch(final long recid, final Serializer<A> serializer)
            throws IOException {

        checkIfClosed();
        if (recid <= 0) {
            throw new IllegalArgumentException("Argument 'recid' is invalid: "
                    + recid);
        }

        if (bufferInUse) {
            //current reusable buffer is in use, have to create new instances
            DataInputOutput buffer2 = new DataInputOutput();
            return fetch2(recid, serializer, buffer2);
        }
        try {
            bufferInUse = true;
            return fetch2(recid, serializer, buffer);
        } finally {
            bufferInUse = false;
        }
    }

    public synchronized <A> A fetch(long recid, Serializer<A> serializer, boolean disableCache) throws IOException {
        //we dont have any cache, so can ignore disableCache parameter
        return fetch(recid, serializer);
    }


    private <A> A fetch2(long recid, final Serializer<A> serializer, final DataInputOutput buf)
            throws IOException {
        buf.reset();
        if(!recman.fetch(recid, buf))
            return null;
        buf.resetForReading();
        try {
            return serializer.deserialize(buf); //TODO there should be write limit to throw EOFException
        } catch (ClassNotFoundException e) {
            throw new IOError(e);
        }
    }
    




    public long getNamedObject(String name)
            throws IOException {
        checkIfClosed();

        Map<String, Long> nameDirectory = getNameDirectory();
        Long recid = (Long) nameDirectory.get(name);
        if (recid == null) {
            return 0;
        }
        return recid.longValue();
    }

    public void setNamedObject(String name, long recid)
            throws IOException {
        checkIfClosed();
        checkCanWrite();

        Map<String, Long> nameDirectory = getNameDirectory();
        if (recid == 0) {
            // remove from hashtable
            nameDirectory.remove(name);
        } else {
            nameDirectory.put(name, new Long(recid));
        }
        saveNameDirectory(nameDirectory);
    }

    public Map<String,Object> getCollections(){
        try{
          Map<String,Object> ret = new LinkedHashMap<String, Object>();
          for(Map.Entry<String,Long> e:getNameDirectory().entrySet()){
              Object o = fetch(e.getValue());
              if(o instanceof BTree){
                  if(((BTree) o).hasValues)
                    o = getTreeMap(e.getKey());
                  else
                    o = getTreeSet(e.getKey());
              }
              else if( o instanceof  HTree){
                  if(((HTree) o).hasValues)
                      o = getHashMap(e.getKey());
                  else
                      o = getHashSet(e.getKey());
              }

            ret.put(e.getKey(), o);
          }
          return Collections.unmodifiableMap(ret);
        }catch(IOException e){
            throw new IOError(e);
        }
                
    }

    
    public void deleteCollection(String name){
        try{
            Map<String,Long> dir = getNameDirectory();
            Long recid = dir.get(name);
            if(recid == null) throw new IOException("Collection not found");
            
            Object c = fetch(recid);
            if(c instanceof  Collection){
                ((Collection)c).clear();
            }else if (c instanceof HTree){
                ((HTree)c).clear();
            }else if (c instanceof BTree){
                ((BTree)c).delete();
            }

            delete(recid);
            
            dir.remove(name);
            saveNameDirectory(dir);

        }catch(IOException e){
            throw new IOError(e);
        }
           
    }


    /**
     * Load name directory
     */
    @SuppressWarnings("unchecked")
    private Map<String, Long> getNameDirectory()
            throws IOException {
        // retrieve directory of named hashtable
        long nameDirectory_recid = recman.getRoot(NAME_DIRECTORY_ROOT);
        if (nameDirectory_recid == 0) {
            _nameDirectory = new HashMap<String, Long>();
            nameDirectory_recid = insert(_nameDirectory);
            recman.setRoot(NAME_DIRECTORY_ROOT, nameDirectory_recid);
        } else {
            _nameDirectory = (Map<String, Long>) fetch(nameDirectory_recid);
        }
        return _nameDirectory;
    }


    private void saveNameDirectory(Map<String, Long> directory)
            throws IOException {
        checkCanWrite();
        long recid = recman.getRoot(NAME_DIRECTORY_ROOT);
        if (recid == 0) {
            throw new IOException("Name directory must exist");
        }
        update(recid, _nameDirectory);
    }


    private Serialization defaultSerializer;

    public synchronized Serializer defaultSerializer() {
        if (defaultSerializer == null) try {
            long serialClassInfoRecid = recman.getRoot(SERIAL_CLASS_INFO_RECID_ROOT);
            if (serialClassInfoRecid == 0) {
                //insert new empty array list
                serialClassInfoRecid = insert(new ArrayList<SerialClassInfo.ClassInfo>(0), SerialClassInfo.serializer,false);
                recman.setRoot(SERIAL_CLASS_INFO_RECID_ROOT, serialClassInfoRecid);
            }

            defaultSerializer = new Serialization(this, serialClassInfoRecid);
        } catch (IOException e) {
            throw new IOError(e);
        }
        return defaultSerializer;
    }


    public synchronized void commit() {
        try {
            checkIfClosed();
            checkCanWrite();
            recman.commit();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }


    public synchronized void rollback() {
        if (transactionsDisabled)
            throw new IllegalAccessError("Transactions are disabled, can not rollback");

        try {
            checkIfClosed();
            recman.rollback();

            defaultSerializer = null;
        } catch (IOException e) {
            throw new IOError(e);
        }

    }



    /**
     * Check if DB has been closed.  If so, throw an
     * IllegalStateException.
     */
    private void checkIfClosed()
            throws IllegalStateException {
        if (recman == null) {
            throw new IllegalStateException("DB has been closed");
        }
    }


    public synchronized void clearCache() {
        //no cache
    }




    public synchronized String calculateStatistics() {
        checkIfClosed();

        try {
            return recman.calculateStatistics();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public synchronized void defrag(boolean sortCollections) {

        try {
            checkIfClosed();
            checkCanWrite();
            recman.defrag(sortCollections);

        } catch (IOException e) {
            throw new IOError(e);
        }

    }
}
