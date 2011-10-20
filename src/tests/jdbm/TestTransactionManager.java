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

package jdbm;

import java.io.File;

import junit.framework.TestSuite;

/**
 *  This class contains all Unit tests for {@link TransactionManager}.
 */
public class TestTransactionManager extends TestCaseWithTestFile {


    String file = newTestFile();
    
    /**
     *  Test constructor. Oops - can only be done indirectly :-)
     */
    public void testCtor() throws Exception {
        RecordFile file2 = new RecordFile(file);

        file2.forceClose();
    }

    /**
     *  Test recovery
     */
    public void XtestRecovery() throws Exception {
        RecordFile file1 = new RecordFile(file);

        // Do three transactions.
        for (int i = 0; i < 3; i++) {
            BlockIo node = file1.get(i);
            node.setDirty();
            file1.release(node);
            file1.commit();
        }
        assertDataSizeEquals("len1", 0);
        assertLogSizeNotZero("len1");

        file1.forceClose();

        // Leave the old record file in flux, and open it again.
        // The second instance should start recovery.
        RecordFile file2 = new RecordFile(file);

        assertDataSizeEquals("len2", 3 * RecordFile.BLOCK_SIZE);
        assertLogSizeEquals("len2", 8);

        file2.forceClose();

        // assure we can recover this log file
        RecordFile file3 = new RecordFile(file);

        file3.forceClose();
    }

    /**
     *  Test background synching
     */
    public void XtestSynching() throws Exception {    	
        RecordFile file1 = new RecordFile(file);

        // Do enough transactions to fill the first slot
        int txnCount = TransactionManager.DEFAULT_TXNS_IN_LOG + 5;
        for (int i = 0; i < txnCount; i++) {
            BlockIo node = file1.get(i);
            node.setDirty();
            file1.release(node);
            file1.commit();
        }
        file1.forceClose();

        // The data file now has the first slotfull
        assertDataSizeEquals("len1", TransactionManager.DEFAULT_TXNS_IN_LOG *
                             RecordFile.BLOCK_SIZE);
        assertLogSizeNotZero("len1");

        // Leave the old record file in flux, and open it again.
        // The second instance should start recovery.
        RecordFile file2 = new RecordFile(file);

        assertDataSizeEquals("len2", txnCount * RecordFile.BLOCK_SIZE);
        assertLogSizeEquals("len2", 8);

        file2.forceClose();
    }

    //  Helpers

    void assertDataSizeEquals(String msg, long size) {
        assertEquals(msg + " data size", size,
                     new File(file
                              + "dbr").length());
    }

    void assertLogSizeEquals(String msg, long size) {
        assertEquals(msg + " log size", size,
                     new File(file
                              + Storage.transaction_log_file_extension).length());
    }

    void assertLogSizeNotZero(String msg) {
        assertTrue(msg + " log size",
               new File(file
                        + Storage.transaction_log_file_extension).length() != 0);
    }

    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(new TestSuite(TestTransactionManager.class));
    }
}
