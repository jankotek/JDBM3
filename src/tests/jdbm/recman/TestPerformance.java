/*
 *  $Id: TestPerformance.java,v 1.4 2003/09/21 15:49:02 boisvert Exp $
 *
 *  Package performance test
 *
 *  Simple db toolkit
 *  Copyright (C) 1999, 2000 Cees de Groot <cg@cdegroot.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Library General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Library General Public License for more details.
 *
 *  You should have received a copy of the GNU Library General Public License
 *  along with this library; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 */
package jdbm.recman;

import java.util.Properties;
import java.util.Random;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.RecordManagerOptions;
import junit.framework.TestCase;
import junit.framework.TestSuite;

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
        RecordManager recman;

        recman = RecordManagerFactory.createRecordManager(testfile);

        int inserts = 0;
        long start = System.currentTimeMillis();
        try {

            long stop = 0;
            while (true) {

                recman.insert(TestUtil.makeRecord(rnd.nextInt(MAXSIZE),
                                               (byte) rnd.nextInt()));
                inserts++;
                
                if ((inserts % 1000) == 0) {
                	recman.commit();
                    stop = System.currentTimeMillis();
                    if (stop - start >= DURATION)
                        break;

                }
            }
            recman.close();
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
        RecordManager recman;
        Properties    options;

        options = new Properties();
        options.setProperty( RecordManagerOptions.DISABLE_TRANSACTIONS, "true" );

        recman = RecordManagerFactory.createRecordManager( testfile,
                                                           options );

        long[] retval = new long[RECORDS];
        System.out.print("Creating test database");
        long start = System.currentTimeMillis();
        try {
            for (int i = 0; i < RECORDS; i++) {
                retval[i] = recman.insert(TestUtil
                                       .makeRecord(rnd.nextInt(MAXSIZE),
                                                   (byte) rnd.nextInt()));
                if ((i % 100) == 0)
                    System.out.print(".");
            }
            recman.close();
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
        RecordManager recman;

        long[] rowids = makeRows();

        recman = RecordManagerFactory.createRecordManager(testfile);;

        int fetches = 0;
        long start = System.currentTimeMillis();
        try {

            long stop = 0;
            while (true) {
                recman.fetch( rowids[ rnd.nextInt( RECORDS ) ] );
                fetches++;
                if ((fetches % 25) == 0) {
                    stop = System.currentTimeMillis();
                    if (stop - start >= DURATION)
                        break;
                }
            }
            recman.close();
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
        RecordManager recman;

        long[] rowids = makeRows();

        recman = RecordManagerFactory.createRecordManager(testfile);

        int updates = 0;
        long start = System.currentTimeMillis();
        try {

            long stop = 0;
            while (true) {

                recman.update(rowids[rnd.nextInt(RECORDS)],
                           TestUtil.makeRecord(rnd.nextInt(MAXSIZE),
                                               (byte) rnd.nextInt()));
                updates++;
                if ((updates % 25) == 0) {
                    stop = System.currentTimeMillis();
                    if (stop - start >= DURATION)
                        break;
                }
            }
            recman.close();
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
        RecordManager recman;

        long[] rowids = makeRows();

        recman = RecordManagerFactory.createRecordManager(testfile);

        int deletes = 0;
        long start = System.currentTimeMillis();
        try {

            long stop = 0;
            // This can be done better...
            for (int i = 0; i < RECORDS; i++) {
                recman.delete(rowids[i]);
                deletes++;
                if ((deletes % 25) == 0) {
                    stop = System.currentTimeMillis();
                    if (stop - start >= 10000)
                        break;
                }
            }
            recman.close();
            System.out.println("Deletes: " + deletes + " in "
                               + (stop - start) + " millisecs");
        } catch (Throwable e) {
            e.printStackTrace();
            fail("unexpected exception after " + deletes + " deletes, "
                 + (System.currentTimeMillis() - start) + "ms: " + e);
        }
    }

    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(new TestSuite(TestPerformance.class));

        // if you just want one test:
        //  TestSuite suite = new TestSuite();
        //  suite.addTest(new TestPerformance("testDeletes"));
        //  junit.textui.TestRunner.run(suite);
    }
}
