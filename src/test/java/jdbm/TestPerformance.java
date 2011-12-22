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

import java.util.Random;

import junit.framework.TestCase;

/**
 *  This class contains performance tests for this package.
 */
public class TestPerformance extends TestCase {

    
    final String testfile = TestCaseWithTestFile.newTestFile();
    // test parameter: maximum record size
    final int MAXSIZE = 500; // is this a reasonable size for real-world apps?
    // test parameter: number of records for fetch/update tests
    final int RECORDS = 10000;
    
    final long DURATION = 10000;

    
    protected void setUp() throws Exception {
    	 new TestCaseWithTestFile(){}.setUp();
    };
    Random rnd = new Random(42);

    /**
     *  Test insert performance
     */
    public void testInserts() throws Exception {


        DBAbstract db = (DBAbstract) new DBMaker(testfile).build();

        int inserts = 0;
        long start = System.currentTimeMillis();
        try {

            long stop = 0;
            while (true) {

                db.insert(UtilTT.makeRecord(rnd.nextInt(MAXSIZE),
                        (byte) rnd.nextInt()));
                inserts++;
                
                if ((inserts % 1000) == 0) {
                	db.commit();
                    stop = System.currentTimeMillis();
                    if (stop - start >= DURATION)
                        break;

                }
            }
            db.close();
            System.out.println("Inserts: " + inserts + " in "
                               + (stop - start) + " millisecs");
        } catch (Throwable e) {
            fail("unexpected exception after " + inserts + " inserts, "
                 + (System.currentTimeMillis() - start) + "ms: " + e);
        }
    }

    /**
     *  Create a database, return array of rowids.
     */
    private long[] makeRows() throws Exception {
        DBAbstract db = (DBAbstract) new DBMaker( testfile).disableTransactions().build();
        long[] retval = new long[RECORDS];
        System.out.print("Creating test database");
        long start = System.currentTimeMillis();
        try {
            for (int i = 0; i < RECORDS; i++) {
                retval[i] = db.insert(UtilTT
                                       .makeRecord(rnd.nextInt(MAXSIZE),
                                               (byte) rnd.nextInt()));
                if ((i % 100) == 0)
                    System.out.print(".");
            }
            db.close();
        } catch (Throwable e) {
            e.printStackTrace();
            fail("unexpected exception during db creation: " + e);
        }

        System.out.println("done (" + RECORDS + " inserts in "
                           + (System.currentTimeMillis() - start) + "ms).");
        return retval;
    }

    /**
     *  Test fetches
     */
    public void testFetches() throws Exception {
        long[] rowids = makeRows();

        DBAbstract db = (DBAbstract) new DBMaker(testfile).build();

        int fetches = 0;
        long start = System.currentTimeMillis();
        try {

            long stop = 0;
            while (true) {
                db.fetch( rowids[ rnd.nextInt( RECORDS ) ] );
                fetches++;
                if ((fetches % 25) == 0) {
                    stop = System.currentTimeMillis();
                    if (stop - start >= DURATION)
                        break;
                }
            }
            db.close();
            System.out.println("Fetches: " + fetches + " in "
                               + (stop - start) + " millisecs");
        } catch (Throwable e) {
            fail("unexpected exception after " + fetches + " fetches, "
                 + (System.currentTimeMillis() - start) + "ms: " + e);
        }
    }

    /**
     *  Test updates.
     */
    public void testUpdates() throws Exception {
        long[] rowids = makeRows();

        DBAbstract db = (DBAbstract) new DBMaker(testfile).build();

        int updates = 0;
        long start = System.currentTimeMillis();
        try {

            long stop = 0;
            while (true) {

                db.update(rowids[rnd.nextInt(RECORDS)],
                           UtilTT.makeRecord(rnd.nextInt(MAXSIZE),
                                   (byte) rnd.nextInt()));
                updates++;
                if ((updates % 25) == 0) {
                    stop = System.currentTimeMillis();
                    if (stop - start >= DURATION)
                        break;
                }
            }
            db.close();
            System.out.println("Updates: " + updates + " in "
                               + (stop - start) + " millisecs");
        } catch (Throwable e) {
            fail("unexpected exception after " + updates + " updates, "
                 + (System.currentTimeMillis() - start) + "ms: " + e);
        }
    }

    /**
     *  Test deletes.
     */
    public void testDeletes() throws Exception {

        long[] rowids = makeRows();

        DBAbstract db = (DBAbstract) new DBMaker( testfile).build();

        int deletes = 0;
        long start = System.currentTimeMillis();
        try {

            long stop = 0;
            // This can be done better...
            for (int i = 0; i < RECORDS; i++) {
                db.delete(rowids[i]);
                deletes++;
                if ((deletes % 25) == 0) {
                    stop = System.currentTimeMillis();
                    if (stop - start >= 10000)
                        break;
                }
            }
            db.close();
            System.out.println("Deletes: " + deletes + " in "
                               + (stop - start) + " millisecs");
        } catch (Throwable e) {
            e.printStackTrace();
            fail("unexpected exception after " + deletes + " deletes, "
                 + (System.currentTimeMillis() - start) + "ms: " + e);
        }
    }


}
