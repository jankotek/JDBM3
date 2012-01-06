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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * This class manages the transaction log that belongs to every
 * {@link RecordFile}. The transaction log is either clean, or
 * in progress. In the latter case, the transaction manager
 * takes care of a roll forward.
 */
// TODO: Handle the case where we are recovering lg9 and lg0, were we
// should start with lg9 instead of lg0!

final class TransactionManager {
    private RecordFile owner;

    // streams for transaction log.
    private DataOutputStream oos;

    /**
     * By default, we keep 10 transactions in the log file before
     * synchronizing it with the main database file.
     */
    static final int DEFAULT_TXNS_IN_LOG = 1;

    /**
     * Maximum number of transactions before the log file is
     * synchronized with the main database file.
     */
    private int _maxTxns = DEFAULT_TXNS_IN_LOG;

    /**
     * In-core copy of transactions. We could read everything back from
     * the log file, but the RecordFile needs to keep the dirty blocks in
     * core anyway, so we might as well point to them and spare us a lot
     * of hassle.
     */
    private ArrayList<BlockIo>[] txns = new ArrayList[DEFAULT_TXNS_IN_LOG];
    private int curTxn = -1;

    private Storage storage;
    private Cipher cipherIn;
    private Cipher cipherOut;

    /**
     * Instantiates a transaction manager instance. If recovery
     * needs to be performed, it is done.
     *
     * @param owner     the RecordFile instance that owns this transaction mgr.
     * @param storage
     * @param cipherIn
     * @param cipherOut
     */
    TransactionManager(RecordFile owner, Storage storage, Cipher cipherIn, Cipher cipherOut) throws IOException {
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
     * Set the maximum number of transactions to record in
     * the log (and keep in memory) before the log is
     * synchronized with the main database file.
     * <p/>
     * This method must be called while there are no
     * pending transactions in the log.
     */
    public void setMaximumTransactionsInLog(int maxTxns)
            throws IOException {
        if (maxTxns <= 0) {
            throw new IllegalArgumentException(
                    "Argument 'maxTxns' must be greater than 0.");
        }
        if (curTxn != -1) {
            throw new IllegalStateException(
                    "Cannot change setting while transactions are pending in the log");
        }
        _maxTxns = maxTxns;
        txns = new ArrayList[maxTxns];
    }


    /**
     * Synchs in-core transactions to data file and opens a fresh log
     */
    private void synchronizeLogFromMemory() throws IOException {
        close();

        TreeSet<BlockIo> blockList = new TreeSet<BlockIo>(BLOCK_IO_COMPARTOR);

        int numBlocks = 0;
        int writtenBlocks = 0;
        for (int i = 0; i < _maxTxns; i++) {
            if (txns[i] == null)
                continue;
            // Add each block to the blockList, replacing the old copy of this
            // block if necessary, thus avoiding writing the same block twice
            for (Iterator<BlockIo> k = txns[i].iterator(); k.hasNext(); ) {
                BlockIo block = k.next();
                if (blockList.contains(block)) {
                    block.decrementTransactionCount();
                } else {
                    writtenBlocks++;
                    boolean result = blockList.add(block);
                }
                numBlocks++;
            }

            txns[i] = null;
        }
        // Write the blocks from the blockList to disk
        synchronizeBlocks(blockList, true);

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
            ArrayList<BlockIo> blocks = null;
            try {
                int size = LongPacker.unpackInt(ois);
                blocks = new ArrayList<BlockIo>(size);
                for (int i = 0; i < size; i++) {
                    BlockIo b = new BlockIo();
                    b.readExternal(ois, cipherOut);
                    blocks.add(b);
                }
            } catch (IOException e) {
                // corrupted logfile, ignore rest of transactions
                break;
            }
            synchronizeBlocks(blocks, false);

        }
        owner.sync();
        ois.close();
        storage.deleteTransactionLog();
    }

    /**
     * Synchronizes the indicated blocks with the owner.
     */
    private void synchronizeBlocks(Iterable<BlockIo> blocks, boolean fromCore)
            throws IOException {
        // write block vector elements to the data file.
        for (BlockIo cur : blocks) {
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
     * Set clean flag on the blocks.
     */
    private void setClean(ArrayList<BlockIo> blocks)
            throws IOException {
        for (BlockIo cur : blocks) {
            cur.setClean();
        }
    }

    /**
     * Discards the indicated blocks and notify the owner.
     */
    private void discardBlocks(ArrayList<BlockIo> blocks)
            throws IOException {
        for (BlockIo cur : blocks) {

            cur.decrementTransactionCount();
            if (!cur.isInTransaction()) {
                owner.releaseFromTransaction(cur);
            }
        }
    }

    /**
     * Starts a transaction. This can block if all slots have been filled
     * with full transactions, waiting for the synchronization thread to
     * clean out slots.
     */
    void start() throws IOException {
        curTxn++;
        if (curTxn == _maxTxns) {
            synchronizeLogFromMemory();
            curTxn = 0;
        }
        txns[curTxn] = new ArrayList();
    }

    /**
     * Indicates the block is part of the transaction.
     */
    void add(BlockIo block) throws IOException {
        block.incrementTransactionCount();
        txns[curTxn].add(block);
    }

    /**
     * Commits the transaction to the log file.
     */
    void commit() throws IOException {
        ArrayList<BlockIo> blocks = txns[curTxn];
        LongPacker.packInt(oos, blocks.size());
        for (BlockIo block : blocks) {
            block.writeExternal(oos, cipherIn);
        }


        sync();

        // set clean flag to indicate blocks have been written to log
        setClean(txns[curTxn]);

        // open a new ObjectOutputStream in order to store
        // newer states of BlockIo
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

        for (int i = 0; i < _maxTxns; i++) {
            if (txns[i] == null)
                continue;
            discardBlocks(txns[i]);
            txns[i] = null;
        }

        recover();
        open();
    }


    /**
     * INNER CLASS.
     * Comparator class for use by the tree set used to store the blocks
     * to write for this transaction.  The BlockIo objects are ordered by
     * their blockIds.
     */
    private static final Comparator<BlockIo> BLOCK_IO_COMPARTOR = new Comparator<BlockIo>() {

        public int compare(BlockIo block1, BlockIo block2) {

            if (block1.getBlockId() == block2.getBlockId()) {
                return 0;
            } else if (block1.getBlockId() < block2.getBlockId()) {
                return -1;
            } else {
                return 1;
            }
        }

    };

}
