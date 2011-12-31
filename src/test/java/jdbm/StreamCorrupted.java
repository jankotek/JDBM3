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

import java.io.IOException;

/**
 * Contributed test case for BTree by Christof Dallermassl (cdaller iicm.edu):
 * <p/>
 * -= quote from original message posted on jdbm-general =-
 * <pre>
 *
 * I tried to insert a couple of elements into a BTree and then remove
 * them one by one. After a number or removals, there is always (if more
 * than 20 elements in btree) a java.io.StreamCorruptedException thrown.
 *
 * The strange thing is, that on 50 elements, the exception is thrown
 * after removing 22, on 200 it is thrown after 36, on 1000 it is thrown
 * after 104, on 10000 it is thrown after 1003....
 *
 * The full stackTrace is here:
 * ---------------------- snip ------- snap -------------------------
 * java.io.StreamCorruptedException: Caught EOFException while reading the
 * stream header
 *   at java.io.ObjectInputStream.readStreamHeader(ObjectInputStream.java:845)
 *   at java.io.ObjectInputStream.<init>(ObjectInputStream.java:168)
 *   at jdbm.db.DB.byteArrayToObject(DB.java:296)
 *   at jdbm.db.DB.fetchObject(DB.java:239)
 *   at jdbm.helper.ObjectCache.fetchObject(ObjectCache.java:104)
 *   at jdbm.btree.BPage.loadBPage(BPage.java:670)
 *   at jdbm.btree.BPage.remove(BPage.java:492)
 *   at jdbm.btree.BPage.remove(BPage.java:437)
 *   at jdbm.btree.BTree.remove(BTree.java:313)
 *   at JDBMTest.main(JDBMTest.java:41)
 *
 * </pre>
 *
 * @author <a href="mailto:cdaller iicm.edu">Christof Dallermassl</a>
 */
public class StreamCorrupted
        extends TestCaseWithTestFile {


    /**
     * Basic tests
     */
    public void testStreamCorrupted()
            throws IOException {
        DBAbstract db;
        BTree btree;
        int iterations;

        iterations = 100; // 23 works :-(((((

        // open database
        db = newRecordManager();

        // create a new B+Tree data structure
        btree = BTree.createInstance(db);
        db.setNamedObject("testbtree", btree.getRecid());

        // action:

        // insert data
        for (int count = 0; count < iterations; count++) {
            btree.insert("num" + count, new Integer(count), true);
        }

        // delete data
        for (int count = 0; count < iterations; count++) {
            btree.remove("num" + count);
        }

        // close database
        db.close();
        db = null;
    }


}
