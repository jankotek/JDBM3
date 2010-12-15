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

package jdbm.recman;

import java.util.Random;

/**
 *  This class contains stress tests for this package.
 */
public class TestStress extends TestCaseWithTestFile {

    // test parameters
    final int RECORDS = 10000;
    final int MAXSIZE = 500;
    final int ROUNDS = 1 * 1000 * 1000;

    final int RPPROMILLE = ROUNDS / 1000;

    Random rnd = new Random(42);


    // holder for record data so we can compare
    class RecordData {
        long rowid;
        int size;
        byte b;

        RecordData(long rowid, int size, byte b) {
            this.rowid = rowid;
            this.size = size;
            this.b = b;
        }

        public String toString() {
            return "slot(" + rowid + ",sz=" + size + ",b=" + b + ")";
        }
    }

    private int getRandomAllocatedSlot(RecordData[] d) {
        int slot = rnd.nextInt(RECORDS);
        while (d[slot] == null) {
            slot++;
            if (slot == RECORDS)
                slot = 0; // wrap
        }
        return slot;
    }

    // holder for root records
    long[] roots = new long[FileHeader.NROOTS];

    private int getRandomAllocatedRoot() {
        int slot = rnd.nextInt(FileHeader.NROOTS);
        while (roots[slot] == 0) {
            slot++;
            if (slot == FileHeader.NROOTS)
                slot = 0; // wrap
        }
        return slot;
    }

    /**
     *  Test basics
     */
    public void testBasics() throws Exception {        

        String file = newTestFile();
        BaseRecordManager recman = new BaseRecordManager(file);

        // as this code is meant to test data structure calculcations
        // and stuff like that, we may want to disable transactions
        // that just slow us down.
        //  mgr.disableTransactions();

        RecordData[] d = new RecordData[RECORDS];
        int recordCount = 0, rootCount = 0;
        int inserts = 0, updates = 0, deletes = 0, fetches = 0;
        int rootgets = 0, rootsets = 0;
        int slot = -1;

        try {

            for (int i = 0; i < ROUNDS; i++) {
                if ((i % RPPROMILLE) == 0)
                    System.out.print("\rComplete: "
                        + i/RPPROMILLE + "/1000th");

                // close and re-open a couple of times during the
                // test, in order to check flushing etcetera.
                if ((i % (ROUNDS / 5)) == 0) {
                    System.out.print(" (reopened at round "
                    + i/RPPROMILLE + ")");
                    recman.close();
                    recman = new BaseRecordManager(file);
                    //        recman.disableTransactions();
                }

                // generate a random number and assign ranges to operations:
                // 0-10 = insert, 20 = delete, 30-50 = update, 51 = set root,
                // 52 = get root, rest = fetch.
                int op = rnd.nextInt(100);
                if (op <= 10) {
                    // INSERT RECORD
                    if (recordCount == RECORDS) {
                        i -= 1;
                        continue;
                    }

                    slot = 0;
                    while (d[slot] != null)
                        slot++;

                    d[slot] = new RecordData(0, rnd.nextInt(MAXSIZE),
                    (byte) rnd.nextInt());
                    d[slot].rowid =
                        recman.insert(UtilTT.makeRecord(d[slot].size,
                                d[slot].b));
                    recordCount++;
                    inserts++;
                }
                else if (op == 20) {
                    // DELETE RECORD
                    if (recordCount == 0) {
                        i -= 1;
                        continue;
                    }

                    slot = getRandomAllocatedSlot(d);
                    recman.delete(d[slot].rowid);
                    d[slot] = null;
                    recordCount--;
                    deletes++;
                }
                else if (op <= 50) {
                    // UPDATE RECORD
                    if (recordCount == 0) {
                        i -= 1;
                        continue;
                    }

                    slot = getRandomAllocatedSlot(d);
                    d[slot].size = rnd.nextInt(MAXSIZE);
                    d[slot].b = (byte) rnd.nextInt();
                    recman.update(d[slot].rowid,
                    UtilTT.makeRecord(d[slot].size,
                            d[slot].b));
                    updates++;
                }
                else if (op == 51) {
                	
                    // SET ROOT
                    int root = rnd.nextInt(FileHeader.NROOTS);
                    if(root>10){ //DONT do this for reserved roots
                    	roots[root] = rnd.nextLong();
                    	recman.setRoot(root, roots[root]);
                    	rootsets++;
                    }
                }
                else if (op == 52) {
                    // GET ROOT
                    if (rootCount == 0) {
                        i -= 1;
                        continue;
                    }

                    int root = getRandomAllocatedRoot();
                    if(root>10){ //DONT do this for reserved roots
                    	assertEquals("root", roots[root], recman.getRoot(root));
                    	rootgets++;
                    }
                }
                else {
                    // FETCH RECORD
                    if (recordCount == 0) {
                        i -= 1;
                        continue;
                    }

                    slot = getRandomAllocatedSlot(d);
                    byte[] data = (byte[]) recman.fetch(d[slot].rowid);
                    assertTrue("fetch round=" + i + ", slot=" + slot
                    + ", " + d[slot],
                    UtilTT.checkRecord(data, d[slot].size, d[slot].b));
                    fetches++;
                }
            }
            recman.close();
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("aborting test at slot " + slot + ": ", e);
        } finally {
            System.out.println("records : " + recordCount);
            System.out.println("deletes : " + deletes);
            System.out.println("inserts : " + inserts);
            System.out.println("updates : " + updates);
            System.out.println("fetches : " + fetches);
            System.out.println("rootget : " + rootgets);
            System.out.println("rootset : " + rootsets);
            int totalSize = 0;
            for (int i = 0; i < RECORDS; i++)
            if (d[i] != null)
                totalSize += d[i].size;
            System.out.println("total outstanding size: " + totalSize);

            //System.out.println("---");
            //for (int i = 0; i < RECORDS; i++)
            //    if (d[i] != null)
            //        System.out.println("slot " + i + ": " + d[i]);
        }

    }


}
