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

import java.io.IOException;

/**
 * Test BTree insert performance.
 */
public class TestInsertPerf extends TestCaseWithTestFile {


    int _numberOfObjects = 1000;


    public void testInsert() throws IOException {


        long start, finish;
        DBAbstract db = newDBCache();
        BTree btree = BTree.createInstance(db);

        // Note:  One can use specialized serializers for better performance / database size
        // btree = BTree.createInstance( db, new LongComparator(),
        //                               LongSerializer.INSTANCE, IntegerSerializer.INSTANCE );

        start = System.currentTimeMillis();
        for (int i = 0; i < _numberOfObjects; i++) {
            btree.insert(new Long(i), new Integer(i), false);
        }
        db.commit();
        finish = System.currentTimeMillis();

        System.out.println("It took " + (finish - start) + " ms to insert "
                + _numberOfObjects + " objects.");

    }


}
