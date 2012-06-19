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
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

/**
 * This class represents a random access file as a set of fixed size
 * records. Each record has a physical record number, and records are
 * cached in order to improve access.
 * <p/>
 * The set of dirty records on the in-use list constitutes a transaction.
 * Later on, we will send these records to some recovery thingy.
 * <p/>
 * PageFile is splited between more files, each with max size 1GB.
 */
final class PageFile {
    final PageTransactionManager txnMgr;


    /**
     * Pages currently locked for read/update ops. When released the page goes
     * to the dirty or clean list, depending on a flag.  The file header page  is
     * normally locked plus the page that is currently being read or modified.
     *
     * @see PageIo#isDirty()
     */
    private final LongHashMap<PageIo> inUse = new LongHashMap<PageIo>();

    /**
     * Pages whose state is dirty.
     */
    private final LongHashMap<PageIo> dirty = new LongHashMap<PageIo>();
    /**
     * Pages in a <em>historical</em> transaction(s) that have been written
     * onto the log but which have not yet been committed to the database.
     */
    private final LongHashMap<PageIo> inTxn = new LongHashMap<PageIo>();


    // transactions disabled?
    final boolean transactionsDisabled;

    /**
     * A array of clean data to wipe clean pages.
     */
    static final byte[] CLEAN_DATA = new byte[Storage.PAGE_SIZE];


    final Storage storage;
    private Cipher cipherOut;
    private Cipher cipherIn;


    /**
     * Creates a new object on the indicated filename. The file is
     * opened in read/write mode.
     *
     * @param fileName the name of the file to open or create, without
     *                 an extension.
     * @throws IOException whenever the creation of the underlying
     *                     RandomAccessFile throws it.
     */
    PageFile(String fileName, boolean readonly, boolean transactionsDisabled, Cipher cipherIn, Cipher cipherOut, boolean useRandomAccessFile, boolean lockingDisabled) throws IOException {
        this.cipherIn = cipherIn;
        this.cipherOut = cipherOut;
        this.transactionsDisabled = transactionsDisabled;
        if(fileName == null){
            this.storage = new StorageMemory(transactionsDisabled);
        }else if(DBMaker.isZipFileLocation(fileName)!=null)
                this.storage = new StorageZip(DBMaker.isZipFileLocation(fileName));
//        }else if (fileName.contains("!/"))
//            this.storage = new StorageZip(fileName);
        else if(useRandomAccessFile)
            this.storage = new StorageDisk(fileName,readonly,lockingDisabled);
        else
            this.storage = new StorageDiskMapped(fileName,readonly,transactionsDisabled,lockingDisabled);

        if (this.storage.isReadonly() && !readonly)
            throw new IllegalArgumentException("This type of storage is readonly, you should call readonly() on DBMaker");
        if (!readonly && !transactionsDisabled) {
            txnMgr = new PageTransactionManager(this, storage, cipherIn, cipherOut);
        } else {
            txnMgr = null;
        }
    }

    public PageFile(String filename) throws IOException {
        this(filename, false, false, null, null,false,false);
    }


    /**
     * Gets a page from the file. The returned byte array is
     * the in-memory copy of the record, and thus can be written
     * (and subsequently released with a dirty flag in order to
     * write the page back). If transactions are disabled, changes
     * may be written directly
     *
     * @param pageId The record number to retrieve.
     */
    PageIo get(long pageId) throws IOException {

        // try in transaction list, dirty list, free list
        PageIo node = inTxn.get(pageId);
        if (node != null) {
            inTxn.remove(pageId);
            inUse.put(pageId, node);
            return node;
        }
        node = dirty.get(pageId);
        if (node != null) {
            dirty.remove(pageId);
            inUse.put(pageId, node);
            return node;
        }


        // sanity check: can't be on in use list
        if (inUse.get(pageId) != null) {
            throw new Error("double get for page " + pageId);
        }

        //read node from file
        if (cipherOut == null) {
            node = new PageIo(pageId,storage.read(pageId));
        } else {
            //decrypt if needed
            ByteBuffer b = storage.read(pageId);
            byte[] bb;
            if(b.hasArray()){
                bb = b.array();
            }else{
                bb = new byte[Storage.PAGE_SIZE];
                b.position(0);
                b.get(bb, 0, Storage.PAGE_SIZE);
            }
            if (!Utils.allZeros(bb)) try {
                bb = cipherOut.doFinal(bb);
                node = new PageIo(pageId, ByteBuffer.wrap(bb));
                } catch (Exception e) {
                throw new IOError(e);
            }else {
                node = new PageIo(pageId, ByteBuffer.wrap(PageFile.CLEAN_DATA).asReadOnlyBuffer());
            }
        }


        inUse.put(pageId, node);
        node.setClean();
        return node;
    }


    /**
     * Releases a page.
     *
     * @param pageId The record number to release.
     * @param isDirty If true, the page was modified since the get().
     */
    void release(final long pageId, final boolean isDirty) throws IOException {

        final PageIo page = inUse.remove(pageId);
        if (!page.isDirty() && isDirty)
            page.setDirty();

        if (page.isDirty()) {
            dirty.put(pageId, page);
        } else if (!transactionsDisabled && page.isInTransaction()) {
            inTxn.put(pageId, page);
        }
    }

    /**
     * Releases a page.
     *
     * @param page The page to release.
     */
    void release(final PageIo page) throws IOException {
        final long key = page.getPageId();
        inUse.remove(key);
        if (page.isDirty()) {
            // System.out.println( "Dirty: " + key + page );
            dirty.put(key, page);
        } else if (!transactionsDisabled && page.isInTransaction()) {
            inTxn.put(key, page);
        }
    }

    /**
     * Discards a page (will not write the page even if it's dirty)
     *
     * @param page The page to discard.
     */
    void discard(PageIo page) {
        long key = page.getPageId();
        inUse.remove(key);
    }

    /**
     * Commits the current transaction by flushing all dirty buffers
     * to disk.
     */
    void commit() throws IOException {
        // debugging...
        if (!inUse.isEmpty() && inUse.size() > 1) {
            showList(inUse.valuesIterator());
            throw new Error("in use list not empty at commit time ("
                    + inUse.size() + ")");
        }

        //  System.out.println("committing...");

        if (dirty.size() == 0) {
            // if no dirty pages, skip commit process
            return;
        }

        if (!transactionsDisabled) {
            txnMgr.start();
        }

        //sort pages by IDs
        long[] pageIds = new long[dirty.size()];
        int c = 0;
        for (Iterator<PageIo> i = dirty.valuesIterator(); i.hasNext(); ) {
            pageIds[c] = i.next().getPageId();
            c++;
        }
        Arrays.sort(pageIds);

        for (long pageId : pageIds) {
            PageIo node = dirty.get(pageId);

            // System.out.println("node " + node + " map size now " + dirty.size());
            if (transactionsDisabled) {
                if(cipherIn !=null)                    
                   storage.write(node.getPageId(), ByteBuffer.wrap(Utils.encrypt(cipherIn, node.getData())));
                else
                   storage.write(node.getPageId(),node.getData());
                node.setClean();
            } else {
                txnMgr.add(node);
                inTxn.put(node.getPageId(), node);
            }
        }
        dirty.clear();
        if (!transactionsDisabled) {
            txnMgr.commit();
        }
    }


    /**
     * Rollback the current transaction by discarding all dirty buffers
     */
    void rollback() throws IOException {
        // debugging...
        if (!inUse.isEmpty()) {
            showList(inUse.valuesIterator());
            throw new Error("in use list not empty at rollback time ("
                    + inUse.size() + ")");
        }
        //  System.out.println("rollback...");
        dirty.clear();

        txnMgr.synchronizeLogFromDisk();

        if (!inTxn.isEmpty()) {
            showList(inTxn.valuesIterator());
            throw new Error("in txn list not empty at rollback time ("
                    + inTxn.size() + ")");
        }
        ;
    }

    /**
     * Commits and closes file.
     */
    void close() throws IOException {
        if (!dirty.isEmpty()) {
            commit();
        }

        if(!transactionsDisabled && txnMgr!=null){
            txnMgr.shutdown();
        }

        if (!inTxn.isEmpty()) {
            showList(inTxn.valuesIterator());
            throw new Error("In transaction not empty");
        }

        // these actually ain't that bad in a production release
        if (!dirty.isEmpty()) {
            System.out.println("ERROR: dirty pages at close time");
            showList(dirty.valuesIterator());
            throw new Error("Dirty pages at close time");
        }
        if (!inUse.isEmpty()) {
            System.out.println("ERROR: inUse pages at close time");
            showList(inUse.valuesIterator());
            throw new Error("inUse pages  at close time");
        }

        storage.sync();
        storage.forceClose();
    }


    /**
     * Force closing the file and underlying transaction manager.
     * Used for testing purposed only.
     */
    void forceClose() throws IOException {
        if(!transactionsDisabled){
            txnMgr.forceClose();
        }
        storage.forceClose();
    }

    /**
     * Prints contents of a list
     */
    private void showList(Iterator<PageIo> i) {
        int cnt = 0;
        while (i.hasNext()) {
            System.out.println("elem " + cnt + ": " + i.next());
            cnt++;
        }
    }

    /**
     * Synchs a node to disk. This is called by the transaction manager's
     * synchronization code.
     */
    void synch(PageIo node) throws IOException {
        ByteBuffer data = node.getData();
        if (data != null) {
            if(cipherIn!=null)
                storage.write(node.getPageId(), ByteBuffer.wrap(Utils.encrypt(cipherIn, data)));
            else
                storage.write(node.getPageId(),  data);
        }
    }

    /**
     * Releases a node from the transaction list, if it was sitting
     * there.
     */
    void releaseFromTransaction(PageIo node)
            throws IOException {
        inTxn.remove(node.getPageId());
    }

    /**
     * Synchronizes the file.
     */
    void sync() throws IOException {
        storage.sync();
    }

    public int getDirtyPageCount() {
        return dirty.size();
    }

    public void deleteAllFiles() throws IOException {
        storage.deleteAllFiles();
    }
}
