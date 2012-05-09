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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * This class manages the transaction log that belongs to every
 * {@link PageFile}. The transaction log is either clean, or
 * in progress. In the latter case, the transaction manager
 * takes care of a roll forward.
 */
// TODO: Handle the case where we are recovering lg9 and lg0, were we
// should start with lg9 instead of lg0!

final class PageTransactionManager {
    private PageFile owner;

    // streams for transaction log.
    private DataOutputStream oos;


    /**
     * In-core copy of transactions. We could read everything back from
     * the log file, but the PageFile needs to keep the dirty pages in
     * core anyway, so we might as well point to them and spare us a lot
     * of hassle.
     */
    private ArrayList<PageIo> txn = new ArrayList<PageIo>();
    private int curTxn = -1;

    private Storage storage;
    private Cipher cipherIn;
    private Cipher cipherOut;

    /**
     * Instantiates a transaction manager instance. If recovery
     * needs to be performed, it is done.
     *
     * @param owner     the PageFile instance that owns this transaction mgr.
     * @param storage
     * @param cipherIn
     * @param cipherOut
     */
    PageTransactionManager(PageFile owner, Storage storage, Cipher cipherIn, Cipher cipherOut) throws IOException {
        this.owner = owner;
        this.storage = storage;
        this.cipherIn = cipherIn;
        this.cipherOut = cipherOut;
        recover();
        open();
    }


    /**
     * Synchronize log file data with the main database file.
     * <p/>
     * After this call, the main database file is guaranteed to be
     * consistent and guaranteed to be the only file needed for
     * backup purposes.
     */
    public void synchronizeLog()
            throws IOException {
        synchronizeLogFromMemory();
    }




    /**
     * Synchs in-core transactions to data file and opens a fresh log
     */
    private void synchronizeLogFromMemory() throws IOException {
        close();

        TreeSet<PageIo> pageList = new TreeSet<PageIo>(PAGE_IO_COMPARTOR);

        int numPages = 0;
        int writtenPages = 0;

        if(txn!=null){
            // Add each page to the pageList, replacing the old copy of this
            // page if necessary, thus avoiding writing the same page twice
            for (Iterator<PageIo> k = txn.iterator(); k.hasNext(); ) {
                PageIo page = k.next();
                if (pageList.contains(page)) {
                    page.decrementTransactionCount();
                } else {
                    writtenPages++;
                    boolean result = pageList.add(page);
                }
                numPages++;
            }

            txn = null;
        }

        // Write the page from the pageList to disk
        synchronizePages(pageList, true);

        owner.sync();
        open();
    }


    /**
     * Opens the log file
     */
    private void open() throws IOException {

        oos = storage.openTransactionLog();
        oos.writeShort(Magic.LOGFILE_HEADER);
        oos.flush();
        curTxn = -1;
    }

    /**
     * Startup recovery on all files
     */
    private void recover() throws IOException {

        DataInputStream ois = storage.readTransactionLog();

        // if transaction log is empty, or does not exist
        if (ois == null) return;

        while (true) {
            ArrayList<PageIo> pages = null;
            try {
                int size = LongPacker.unpackInt(ois);
                pages = new ArrayList<PageIo>(size);
                for (int i = 0; i < size; i++) {
                    PageIo b = new PageIo();
                    b.readExternal(ois, cipherOut);
                    pages.add(b);
                }
            } catch (IOException e) {
                // corrupted logfile, ignore rest of transactions
                break;
            }
            synchronizePages(pages, false);

        }
        owner.sync();
        ois.close();
        storage.deleteTransactionLog();
    }

    /**
     * Synchronizes the indicated pages with the owner.
     */
    private void synchronizePages(Iterable<PageIo> pages, boolean fromCore)
            throws IOException {
        // write pages vector elements to the data file.
        for (PageIo cur : pages) {
            owner.synch(cur);
            if (fromCore) {
                cur.decrementTransactionCount();
                if (!cur.isInTransaction()) {
                    owner.releaseFromTransaction(cur);
                }
            }
        }
    }


    /**
     * Set clean flag on the pages.
     */
    private void setClean(ArrayList<PageIo> pages)
            throws IOException {
        for (PageIo cur : pages) {
            cur.setClean();
        }
    }

    /**
     * Discards the indicated pages and notify the owner.
     */
    private void discardPages(ArrayList<PageIo> pages)
            throws IOException {
        for (PageIo cur : pages) {

            cur.decrementTransactionCount();
            if (!cur.isInTransaction()) {
                owner.releaseFromTransaction(cur);
            }
        }
    }

    /**
     * Starts a transaction. This can pages if all slots have been filled
     * with full transactions, waiting for the synchronization thread to
     * clean out slots.
     */
    void start() throws IOException {
        curTxn++;
        if (curTxn == 1) {
            synchronizeLogFromMemory();
            curTxn = 0;
        }
        txn = new ArrayList();
    }

    /**
     * Indicates the page is part of the transaction.
     */
    void add(PageIo page) throws IOException {
        page.incrementTransactionCount();
        txn.add(page);
    }

    /**
     * Commits the transaction to the log file.
     */
    void commit() throws IOException {
        LongPacker.packInt(oos, txn.size());
        for (PageIo page : txn) {
            page.writeExternal(oos, cipherIn);
        }


        sync();

        // set clean flag to indicate pages have been written to log
        setClean(txn);

        // open a new ObjectOutputStream in order to store
        // newer states of PageIo
//        oos = new DataOutputStream(new BufferedOutputStream(fos));
    }

    /**
     * Flushes and syncs
     */
    private void sync() throws IOException {
        oos.flush();
    }

    /**
     * Shutdowns the transaction manager. Resynchronizes outstanding
     * logs.
     */
    void shutdown() throws IOException {
        synchronizeLogFromMemory();
        close();
    }

    /**
     * Closes open files.
     */
    private void close() throws IOException {
        sync();
        oos.close();
        oos = null;
    }

    /**
     * Force closing the file without synchronizing pending transaction data.
     * Used for testing purposes only.
     */
    void forceClose() throws IOException {
        oos.close();
        oos = null;
    }

    /**
     * Use the disk-based transaction log to synchronize the data file.
     * Outstanding memory logs are discarded because they are believed
     * to be inconsistent.
     */
    void synchronizeLogFromDisk() throws IOException {
        close();


        if (txn != null){
            discardPages(txn);
            txn = null;
        }

        recover();
        open();
    }


    /**
     * INNER CLASS.
     * Comparator class for use by the tree set used to store the pages
     * to write for this transaction.  The PageIo objects are ordered by
     * their page ids.
     */
    private static final Comparator<PageIo> PAGE_IO_COMPARTOR = new Comparator<PageIo>() {

        public int compare(PageIo page1, PageIo page2) {

            if (page1.getPageId() == page2.getPageId()) {
                return 0;
            } else if (page1.getPageId() < page2.getPageId()) {
                return -1;
            } else {
                return 1;
            }
        }

    };

}
