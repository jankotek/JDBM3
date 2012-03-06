package net.kotek.jdbm;

import javax.crypto.Cipher;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Implemets  RecordManager using JDBM storage
 */
class RecordManagerNative implements RecordManager {

    private static final int AUTOCOMMIT_AFTER_N_PAGES = 1024 * 5;


    /**
     * Underlying file for store records.
     */
    private RecordFile _file;

    /**
     * Page manager for physical manager.
     */
    PageManager _pageman;

    /**
     * Physical row identifier manager.
     */
    private PhysicalRowIdManager _physMgr;

    /**
     * Logigal to Physical row identifier manager.
     */
    private LogicalRowIdManager _logicMgr;


    final private boolean transactionsDisabled;
    final private boolean autodefrag;
    final private String fileName;
    final private boolean useRandomAccessFile;
    final private boolean readonly;
    final private Cipher cipherIn;
    final private Cipher cipherOut;


    RecordManagerNative(String fileName, boolean readonly,  boolean transactionsDisabled, Cipher cipherIn, Cipher cipherOut, boolean useRandomAccessFile, boolean autodefrag) throws IOException {
        this.fileName = fileName;
        this.readonly = readonly;
        this.transactionsDisabled = transactionsDisabled;
        this.cipherIn = cipherIn;
        this.cipherOut = cipherOut;
        this.useRandomAccessFile = useRandomAccessFile;
        this.autodefrag = autodefrag;


        reopen();
    }


    public long insert(byte[] rec, int pos, int length) throws IOException {
        final long physRowId = _physMgr.insert(rec, pos, length);
        final long recid = _logicMgr.insert(physRowId);

        return Location.compressRecid(recid);
    }

    public void update(long recid, byte[] rec, int pos, int length) throws IOException {
        recid =  Location.decompressRecid(recid);

        long physRecid = _logicMgr.fetch(recid);
        if (physRecid == 0)
            throw new IOException("Can not update, recid does not exist: " + recid);


        long newRecid = _physMgr.update(physRecid,rec,pos, length);

        _logicMgr.update(recid, newRecid);
    }

    public void delete(long recid) throws IOException {
        recid =  Location.decompressRecid(recid);

        long physRowId = _logicMgr.fetch(recid);
        _physMgr.free(physRowId);
        _logicMgr.delete(recid);

    }

    public boolean fetch(long recid, DataOutput buf) throws IOException {
        recid =  Location.decompressRecid(recid);

        long physLocation = _logicMgr.fetch(recid);
        if (physLocation == 0) {
            //throw new IOException("Record not found, recid: "+recid);
            return false;
        }
        _physMgr.fetch(buf, physLocation);
        return true;

    }

    public boolean supportsTransaction() {
        return !transactionsDisabled;
    }

    public void commit() throws IOException {
        /** flush free phys rows into pages*/
        _physMgr.commit();
        _logicMgr.commit();

        /**commit pages */
        _pageman.commit();


        if(autodefrag && _physMgr.freeman.needsDefragementation){

            _physMgr.freeman.needsDefragementation = false;
            defrag(false);
        }

    }



    public void rollback() throws IOException {
        _physMgr.rollback();
        _logicMgr.rollback();
        _pageman.rollback();
    }

    public void close() throws IOException {
        _pageman.close();
        _file.close();
    }



    public String calculateStatistics() throws IOException {


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
    }

    public void defrag(boolean fullDefrag) throws IOException {
        commit();
        final String filename2 = fileName + "_defrag" + System.currentTimeMillis();
        final String filename1 = fileName;
        DBStore db2 = new DBStore(filename2, false, true, _file.cipherIn, _file.cipherOut, false,true);
        RecordManagerNative recman2 = (RecordManagerNative) db2.recman;

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
                    long pageid =recman2._pageman.allocate(Magic.TRANSLATION_PAGE);
                    pageid >= minpageid;
                    pageid = recman2._pageman.allocate(Magic.TRANSLATION_PAGE)
                    ) {
                pageCounter++;
                if (pageCounter % 1000 == 0)
                    db2.commit();
            }

            logicalPages = null;
        }


        //reinsert collections so physical records are located near each other
        //iterate over named object recids, it is sorted with TreeSet
        if(fullDefrag){

            //TODO defrag collections independently on store
//            for (Long namedRecid : new TreeSet<Long>(getNameDirectory().values())) {
//                Object obj = fetch(namedRecid);
//                if (obj instanceof LinkedList) {
//                    LinkedList.defrag(namedRecid, this, db2);
//                } else if (obj instanceof HTree) {
//                    HTree.defrag(namedRecid, this, db2);
//                } else if (obj instanceof BTree) {
//                    BTree.defrag(namedRecid, this, db2);
//                }
//            }
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
                if (recman2._pageman.getLast(Magic.TRANSLATION_PAGE) <= pageid &&
                        recman2._logicMgr.fetch(logicalRowId) != 0) {
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
                long physLoc = recman2._physMgr.insert(bb, 0, bb.length);
                recman2._logicMgr.forceInsert(logicalRowId, physLoc);

            }
            _file.release(io);
            db2.commit();
        }
        recman2.setRoot(DBStore.NAME_DIRECTORY_ROOT, getRoot(DBStore.NAME_DIRECTORY_ROOT));

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

    }

    private void reopen() throws IOException {
        _file = new RecordFile(fileName, readonly, transactionsDisabled, cipherIn, cipherOut,useRandomAccessFile);
        _pageman = new PageManager(_file);
        _physMgr = new PhysicalRowIdManager(_file, _pageman,
                new PhysicalRowIdPageManagerFree(_file, _pageman));

        _logicMgr = new LogicalRowIdManager(_file, _pageman);
    }

    public boolean needsAutoCommit() {
        return transactionsDisabled &&
            (_file.getDirtyPageCount() >= AUTOCOMMIT_AFTER_N_PAGES || _physMgr.freeman.needsDefragementation);
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



    public void copyToZipStore(String zipFile) {
        try {
            String zip = zipFile.substring(0, zipFile.indexOf("!/")); //TODO does not work on windows
            String zip2 = zipFile.substring(zipFile.indexOf("!/") + 2);
            ZipOutputStream z = new ZipOutputStream(new FileOutputStream(zip));

            //copy zero pages
            {
                String file = zip2 + StorageDiskMapped.DBR + 0;
                z.putNextEntry(new ZipEntry(file));
                z.write(Utils.encrypt(_file.cipherIn, _pageman.getHeaderBufData()));
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
                z.write(Utils.encrypt(_file.cipherIn, block.getData()));
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
                z.write(Utils.encrypt(_file.cipherIn, block.getData()));
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
                z.write(Utils.encrypt(_file.cipherIn, block.getData()));
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
                z.write(Utils.encrypt(_file.cipherIn, block.getData()));
                z.closeEntry();
                _file.release(block);
            }
            z.close();

        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    public synchronized long getRoot(int id)
            throws IOException {

        return _pageman.getFileHeader().fileHeaderGetRoot(id);
    }


    public synchronized void setRoot(int id, long rowid)
            throws IOException {

        _pageman.getFileHeader().fileHeaderSetRoot(id, rowid);
    }


    /**
     * Insert data at forced logicalRowId, use only for defragmentation !!
     *
     * @param logicalRowId
     * @param data
     * @throws IOException
     */
    public void forceInsert(long logicalRowId, byte[] data) throws IOException {
        logicalRowId = Location.decompressRecid(logicalRowId);

        if (/*!wrappedInCache &&*/ needsAutoCommit()) {  //TODO wrappedInCache used to be here
            commit();
        }

        long physLoc = _physMgr.insert(data, 0, data.length);
        _logicMgr.forceInsert(logicalRowId, physLoc);
    }

    public byte[] fetchRaw(long recid) throws IOException {
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


}
