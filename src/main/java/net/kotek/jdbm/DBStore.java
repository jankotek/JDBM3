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

    /**
     * Underlying file for store records.
     */
    private RecordFile _file;

    /**
     * Page manager for physical manager.
     */
    private PageManager _pageman;

    /**
     * Physical row identifier manager.
     */
    private PhysicalRowIdManager _physMgr;

    /**
     * Indicated that store is opened for readonly operations
     * If true, store will throw UnsupportedOperationException when update/insert/delete operation is called
     */
    private final boolean readonly;
    private final boolean transactionsDisabled;
    private final boolean autodefrag;
    private final boolean deleteFilesAfterClose;

    private static final int AUTOCOMMIT_AFTER_N_PAGES = 1024 * 5;


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


    void checkCanWrite() {
        if (readonly)
            throw new UnsupportedOperationException("Could not write, store is opened as read-only");
    }




    /**
     * Logigal to Physical row identifier manager.
     */
    private LogicalRowIdManager _logicMgr;


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
                   boolean autodefrag,boolean deleteFilesAfterClose)
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
        _file = new RecordFile(_filename, readonly, transactionsDisabled, cipherIn, cipherOut,useRandomAccessFile);
        _pageman = new PageManager(_file);
        _physMgr = new PhysicalRowIdManager(_file, _pageman,
                new PhysicalRowIdPageManagerFree(_file, _pageman));

        _logicMgr = new LogicalRowIdManager(_file, _pageman);

        long versionNumber = getRoot(STORE_VERSION_NUMBER_ROOT);
        if (versionNumber > STORE_FORMAT_VERSION)
            throw new IOException("Unsupported version of store. Please update JDBM. Minimal supported ver:" + STORE_FORMAT_VERSION + ", store ver:" + versionNumber);
        if (!readonly)
            setRoot(STORE_VERSION_NUMBER_ROOT, STORE_FORMAT_VERSION);

        defaultSerializer = null;

    }


    /**
     * Closes the record manager.
     *
     * @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void close() {
        checkIfClosed();
        try {
            _pageman.close();
            _file.close();
            if(deleteFilesAfterClose)
                _file.storage.deleteAllFiles();
            
            _pageman = null;

            
            _file = null;

        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public boolean isClosed() {
        return _pageman==null;
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
        return  transactionsDisabled &&
                (_file.getDirtyPageCount() >= AUTOCOMMIT_AFTER_N_PAGES || _physMgr.freeman.needsDefragementation);
    }


    private <A> long insert2(A obj, Serializer<A> serializer, DataInputOutput buf)
            throws IOException {
        buf.reset();

        serializer.serialize(buf, obj);
        final long physRowId = _physMgr.insert(buf.getBuf(), 0, buf.getPos());
        final long recid = _logicMgr.insert(physRowId);
        if (DEBUG) {
            System.out.println("BaseRecordManager.insert() recid " + recid + " length " + buf.getPos());
        }

        return Location.compressRecid(recid);
    }


    public synchronized void delete(long logRowId)
            throws IOException {

        checkIfClosed();
        checkCanWrite();
        if (logRowId <= 0) {
            throw new IllegalArgumentException("Argument 'recid' is invalid: "
                    + logRowId);
        }

        if (!wrappedInCache && needsAutoCommit()) {
            commit();
        }

        if (DEBUG) {
            System.out.println("BaseRecordManager.delete() recid " + logRowId);
        }

        logRowId =  Location.decompressRecid(logRowId);

        long physRowId = _logicMgr.fetch(logRowId);
        _physMgr.free(physRowId);
        _logicMgr.delete(logRowId);
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

        logRecid =  Location.decompressRecid(logRecid);

        long physRecid = _logicMgr.fetch(logRecid);
        if (physRecid == 0)
            throw new IOException("Can not update, recid does not exist: " + logRecid);
        buf.reset();
        serializer.serialize(buf, obj);


        if (DEBUG) {
            System.out.println("BaseRecordManager.update() recid " + logRecid + " length " + buf.getPos());
        }

        long newRecid = _physMgr.update(physRecid, buf.getBuf(), 0, buf.getPos());

        _logicMgr.update(logRecid, newRecid);

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

        recid =  Location.decompressRecid(recid);

        buf.reset();
        long physLocation = _logicMgr.fetch(recid);
        if (physLocation == 0) {
            //throw new IOException("Record not found, recid: "+recid);
            return null;
        }
        _physMgr.fetch(buf, physLocation);

        if (DEBUG) {
            System.out.println("BaseRecordManager.fetch() recid " + recid + " length " + buf.getPos());
        }
        buf.resetForReading();
        try {
            return serializer.deserialize(buf); //TODO there should be write limit to throw EOFException
        } catch (ClassNotFoundException e) {
            throw new IOError(e);
        }
    }

    byte[] fetchRaw(long recid) throws IOException {
        recid =  Location.decompressRecid(recid);
        long physLocation = _logicMgr.fetch(recid);
        if (physLocation == 0) {
            //throw new IOException("Record not found, recid: "+recid);
            return null;
        }
        DataInputOutput i = new DataInputOutput();
        _physMgr.fetch(i, physLocation);
        return i.toByteArray();
    }


    public synchronized long getRoot(int id)
            throws IOException {
        checkIfClosed();

        return _pageman.getFileHeader().fileHeaderGetRoot(id);
    }


    public synchronized void setRoot(int id, long rowid)
            throws IOException {
        checkIfClosed();
        checkCanWrite();

        _pageman.getFileHeader().fileHeaderSetRoot(id, rowid);
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

            Collection c = fetch(recid);
            c.clear();
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
        long nameDirectory_recid = getRoot(NAME_DIRECTORY_ROOT);
        if (nameDirectory_recid == 0) {
            _nameDirectory = new HashMap<String, Long>();
            nameDirectory_recid = insert(_nameDirectory);
            setRoot(NAME_DIRECTORY_ROOT, nameDirectory_recid);
        } else {
            _nameDirectory = (Map<String, Long>) fetch(nameDirectory_recid);
        }
        return _nameDirectory;
    }


    private void saveNameDirectory(Map<String, Long> directory)
            throws IOException {
        checkCanWrite();
        long recid = getRoot(NAME_DIRECTORY_ROOT);
        if (recid == 0) {
            throw new IOException("Name directory must exist");
        }
        update(recid, _nameDirectory);
    }


    private Serialization defaultSerializer;

    public synchronized Serializer defaultSerializer() {
        if (defaultSerializer == null) try {
            long serialClassInfoRecid = getRoot(SERIAL_CLASS_INFO_RECID_ROOT);
            if (serialClassInfoRecid == 0) {
                //insert new empty array list
                serialClassInfoRecid = insert(new ArrayList<SerialClassInfo.ClassInfo>(0), SerialClassInfo.serializer,false);
                setRoot(SERIAL_CLASS_INFO_RECID_ROOT, serialClassInfoRecid);
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
            /** flush free phys rows into pages*/
            _physMgr.commit();
            _logicMgr.commit();

            /**commit pages */
            _pageman.commit();


            if(autodefrag && _physMgr.freeman.needsDefragementation){

                _physMgr.freeman.needsDefragementation = false;
                defrag(false);
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }


    public synchronized void rollback() {
        if (transactionsDisabled)
            throw new IllegalAccessError("Transactions are disabled, can not rollback");

        try {
            checkIfClosed();
            _physMgr.rollback();
            _logicMgr.rollback();
            _pageman.rollback();
            defaultSerializer = null;
        } catch (IOException e) {
            throw new IOError(e);
        }

    }

    public void copyToZipStore(String zipFile) {
        try {
            String zip = zipFile.substring(0, zipFile.indexOf("!/")); //TODO does not work on windows
            String zip2 = zipFile.substring(zipFile.indexOf("!/") + 2);
            ZipOutputStream z = new ZipOutputStream(new FileOutputStream(zip));

            //copy zero pages
            {
                String file = zip2 + StorageDiskMapped.DBR + 0;
                z.putNextEntry(new ZipEntry(file));
                z.write(Utils.encrypt(cipherIn, _pageman.getHeaderBufData()));
                z.closeEntry();
            }

            //iterate over pages and create new file for each
            for (long pageid = _pageman.getFirst(Magic.TRANSLATION_PAGE);
                 pageid != 0;
                 pageid = _pageman.getNext(pageid)
                    ) {
                BlockIo block = _file.get(pageid);
                String file = zip2 + StorageDiskMapped.IDR + pageid;
                z.putNextEntry(new ZipEntry(file));
                z.write(Utils.encrypt(cipherIn, block.getData()));
                z.closeEntry();
                _file.release(block);
            }
            for (long pageid = _pageman.getFirst(Magic.FREELOGIDS_PAGE);
                 pageid != 0;
                 pageid = _pageman.getNext(pageid)
                    ) {
                BlockIo block = _file.get(pageid);
                String file = zip2 + StorageDiskMapped.IDR + pageid;
                z.putNextEntry(new ZipEntry(file));
                z.write(Utils.encrypt(cipherIn, block.getData()));
                z.closeEntry();
                _file.release(block);
            }

            for (long pageid = _pageman.getFirst(Magic.USED_PAGE);
                 pageid != 0;
                 pageid = _pageman.getNext(pageid)
                    ) {
                BlockIo block = _file.get(pageid);
                String file = zip2 + StorageDiskMapped.DBR + pageid;
                z.putNextEntry(new ZipEntry(file));
                z.write(Utils.encrypt(cipherIn, block.getData()));
                z.closeEntry();
                _file.release(block);
            }
            for (long pageid = _pageman.getFirst(Magic.FREEPHYSIDS_PAGE);
                 pageid != 0;
                 pageid = _pageman.getNext(pageid)
                    ) {
                BlockIo block = _file.get(pageid);
                String file = zip2 + StorageDiskMapped.DBR + pageid;
                z.putNextEntry(new ZipEntry(file));
                z.write(Utils.encrypt(cipherIn, block.getData()));
                z.closeEntry();
                _file.release(block);
            }
            z.close();

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
        if (_file == null) {
            throw new IllegalStateException("DB has been closed");
        }
    }


    public synchronized void clearCache() {
        //no cache
    }


    private long statisticsCountPages(short pageType) throws IOException {
        long pageCounter = 0;

        for (long pageid = _pageman.getFirst(pageType);
             pageid != 0;
             pageid = _pageman.getNext(pageid)
                ) {
            pageCounter++;
        }


        return pageCounter;

    }

    public synchronized String calculateStatistics() {
        checkIfClosed();

        try {

            final StringBuilder b = new StringBuilder();

            //count pages
            {

                b.append("PAGES:\n");
                long total = 0;
                long pages = statisticsCountPages(Magic.USED_PAGE);
                total += pages;
                b.append("  " + pages + " used pages with size " + Utils.formatSpaceUsage(pages * Storage.BLOCK_SIZE) + "\n");
                pages = statisticsCountPages(Magic.TRANSLATION_PAGE);
                total += pages;
                b.append("  " + pages + " record translation pages with size " + Utils.formatSpaceUsage(pages * Storage.BLOCK_SIZE) + "\n");
                pages = statisticsCountPages(Magic.FREE_PAGE);
                total += pages;
                b.append("  " + pages + " free (unused) pages with size " + Utils.formatSpaceUsage(pages * Storage.BLOCK_SIZE) + "\n");
                pages = statisticsCountPages(Magic.FREEPHYSIDS_PAGE);
                total += pages;
                b.append("  " + pages + " free (phys) pages with size " + Utils.formatSpaceUsage(pages * Storage.BLOCK_SIZE) + "\n");
                pages = statisticsCountPages(Magic.FREELOGIDS_PAGE);
                total += pages;
                b.append("  " + pages + " free (logical) pages with size " + Utils.formatSpaceUsage(pages * Storage.BLOCK_SIZE) + "\n");
                b.append("  Total number of pages is " + total + " with size " + Utils.formatSpaceUsage(total * Storage.BLOCK_SIZE) + "\n");

            }
            {
                b.append("RECORDS:\n");

                long recordCount = 0;
                long freeRecordCount = 0;
                long maximalRecordSize = 0;
                long maximalAvailSizeDiff = 0;
                long totalRecordSize = 0;
                long totalAvailDiff = 0;

                //count records
                for (long pageid = _pageman.getFirst(Magic.TRANSLATION_PAGE);
                     pageid != 0;
                     pageid = _pageman.getNext(pageid)
                        ) {
                    BlockIo io = _file.get(pageid);

                    for (int i = 0; i < _logicMgr.ELEMS_PER_PAGE; i += 1) {
                        final int pos = Magic.PAGE_HEADER_SIZE + i * Magic.PhysicalRowId_SIZE;
                        final long physLoc = io.pageHeaderGetLocation((short) pos);

                        if (physLoc == 0) {
                            freeRecordCount++;
                            continue;
                        }

                        recordCount++;

                        //get size
                        BlockIo block = _file.get(Location.getBlock(physLoc));
                        final short physOffset = Location.getOffset(physLoc);
                        int availSize = RecordHeader.getAvailableSize(block, physOffset);
                        int currentSize = RecordHeader.getCurrentSize(block, physOffset);
                        _file.release(block);

                        maximalAvailSizeDiff = Math.max(maximalAvailSizeDiff, availSize - currentSize);
                        maximalRecordSize = Math.max(maximalRecordSize, currentSize);
                        totalAvailDiff += availSize - currentSize;
                        totalRecordSize += currentSize;

                    }
                    _file.release(io);
                }

                b.append("  Contains " + recordCount + " records and " + freeRecordCount + " free slots.\n");
                b.append("  Total space occupied by data is " + Utils.formatSpaceUsage(totalRecordSize) + "\n");
                b.append("  Average data size in record is " + Utils.formatSpaceUsage(Math.round(1D * totalRecordSize / recordCount)) + "\n");
                b.append("  Maximal data size in record is " + Utils.formatSpaceUsage(maximalRecordSize) + "\n");
                b.append("  Space wasted in record fragmentation is " + Utils.formatSpaceUsage(totalAvailDiff) + "\n");
                b.append("  Maximal space wasted in single record fragmentation is " + Utils.formatSpaceUsage(maximalAvailSizeDiff) + "\n");
            }

            return b.toString();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public synchronized void defrag(boolean sortCollections) {

        try {
            checkIfClosed();
            checkCanWrite();
            commit();
            final String filename2 = _filename + "_defrag" + System.currentTimeMillis();
            final String filename1 = _filename;
            DBStore db2 = new DBStore(filename2, false, true, cipherIn, cipherOut, false,true,false);

            //recreate logical file with original page layout
            {
                //find minimal logical pageid (logical pageids are negative)
                LongHashMap<String> logicalPages = new LongHashMap<String>();
                long minpageid = 0;
                for (long pageid = _pageman.getFirst(Magic.TRANSLATION_PAGE);
                     pageid != 0;
                     pageid = _pageman.getNext(pageid)
                        ) {
                    minpageid = Math.min(minpageid, pageid);
                    logicalPages.put(pageid, Utils.EMPTY_STRING);
                }

                //fill second db with logical pages
                long pageCounter = 0;
                for (
                        long pageid = db2._pageman.allocate(Magic.TRANSLATION_PAGE);
                        pageid >= minpageid;
                        pageid = db2._pageman.allocate(Magic.TRANSLATION_PAGE)
                        ) {
                    pageCounter++;
                    if (pageCounter % 1000 == 0)
                        db2.commit();
                }

                logicalPages = null;
            }


            //reinsert collections so physical records are located near each other
            //iterate over named object recids, it is sorted with TreeSet
            if(sortCollections){
                for (Long namedRecid : new TreeSet<Long>(getNameDirectory().values())) {
                    Object obj = fetch(namedRecid);
                    if (obj instanceof LinkedList) {
                        LinkedList2.defrag(namedRecid, this, db2);
                    } else if (obj instanceof HTree) {
                        HTree.defrag(namedRecid, this, db2);
                    } else if (obj instanceof BTree) {
                        BTree.defrag(namedRecid, this, db2);
                    }
                }
            }


            for (long pageid = _pageman.getFirst(Magic.TRANSLATION_PAGE);
                 pageid != 0;
                 pageid = _pageman.getNext(pageid)
                    ) {
                BlockIo io = _file.get(pageid);

                for (int i = 0; i < _logicMgr.ELEMS_PER_PAGE; i += 1) {
                    final int pos = Magic.PAGE_HEADER_SIZE + i * Magic.PhysicalRowId_SIZE;
                    if (pos > Short.MAX_VALUE)
                        throw new Error();

                    //write to new file
                    final long logicalRowId = Location.toLong(-pageid, (short) pos);

                    //read from logical location in second db,
                    //check if record was already inserted as part of collections
                    if (db2._pageman.getLast(Magic.TRANSLATION_PAGE) <= pageid &&
                            db2._logicMgr.fetch(logicalRowId) != 0) {
                        //yes, this record already exists in second db
                        continue;
                    }

                    //get physical location in this db
                    final long physRowId =  io.pageHeaderGetLocation((short) pos);

                    if (physRowId == 0)
                        continue;

                    //read from physical location at this db
                    DataInputOutput b = new DataInputOutput();
                    _physMgr.fetch(b, physRowId);
                    byte[] bb = b.toByteArray();

                    //force insert into other file, without decompressing logical id to external form
                    long physLoc = db2._physMgr.insert(bb, 0, bb.length);
                    db2._logicMgr.forceInsert(logicalRowId, physLoc);

                }
                _file.release(io);
                db2.commit();
            }
            db2.setRoot(NAME_DIRECTORY_ROOT, getRoot(NAME_DIRECTORY_ROOT));

            db2.close();
            close();

            List<File> filesToDelete = new ArrayList<File>();
            //now rename old files
            String[] exts = {StorageDiskMapped.IDR, StorageDiskMapped.DBR};
            for (String ext : exts) {
                String f1 = filename1 + ext;
                String f2 = filename2 + "_OLD" + ext;

                //first rename transaction log
                File f1t = new File(f1 + StorageDisk.transaction_log_file_extension);
                File f2t = new File(f2 + StorageDisk.transaction_log_file_extension);
                f1t.renameTo(f2t);
                filesToDelete.add(f2t);

                //rename data files, iterate until file exist
                for (int i = 0; ; i++) {
                    File f1d = new File(f1 + "." + i);
                    if (!f1d.exists()) break;
                    File f2d = new File(f2 + "." + i);
                    f1d.renameTo(f2d);
                    filesToDelete.add(f2d);
                }
            }

            //rename new files
            for (String ext : exts) {
                String f1 = filename2 + ext;
                String f2 = filename1 + ext;

                //first rename transaction log
                File f1t = new File(f1 + StorageDisk.transaction_log_file_extension);
                File f2t = new File(f2 + StorageDisk.transaction_log_file_extension);
                f1t.renameTo(f2t);

                //rename data files, iterate until file exist
                for (int i = 0; ; i++) {
                    File f1d = new File(f1 + "." + i);
                    if (!f1d.exists()) break;
                    File f2d = new File(f2 + "." + i);
                    f1d.renameTo(f2d);
                }
            }

            for (File d : filesToDelete) {
                d.delete();
            }


            reopen();
        } catch (IOException e) {
            throw new IOError(e);
        }

    }

    /**
     * Insert data at forced logicalRowId, use only for defragmentation !!
     *
     * @param logicalRowId
     * @param data
     * @throws IOException
     */
    void forceInsert(long logicalRowId, byte[] data) throws IOException {
        logicalRowId = Location.decompressRecid(logicalRowId);

        if (!wrappedInCache && needsAutoCommit()) {
            commit();
        }

        long physLoc = _physMgr.insert(data, 0, data.length);
        _logicMgr.forceInsert(logicalRowId, physLoc);
    }



    /**
     * Returns number of records stored in database.
     * Is used for unit tests
     */
    long countRecords() throws IOException {
        long counter = 0;

        long page = _pageman.getFirst(Magic.TRANSLATION_PAGE);
        while (page != 0) {
            BlockIo io = _file.get(page);
            for (int i = 0; i < _logicMgr.ELEMS_PER_PAGE; i += 1) {
                int pos = Magic.PAGE_HEADER_SIZE + i * Magic.PhysicalRowId_SIZE;
                if (pos > Short.MAX_VALUE)
                    throw new Error();

                //get physical location
                long physRowId = io.pageHeaderGetLocation((short) pos);

                if (physRowId != 0)
                    counter += 1;
            }
            _file.release(io);
            page = _pageman.getNext(page);
        }
        return counter;
    }


}
