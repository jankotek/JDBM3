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
 * This class contains all Unit tests for {@link BTreeNode}.
 *
 * @author Alex Boisvert
 */
public class BTreeNodeTest extends TestCaseWithTestFile {


    /**
     * Basic tests
     */
    public void testBasics() throws IOException {
        DBAbstract db;
        String test, test1, test2, test3;

        test = "test";
        test1 = "test1";
        test2 = "test2";
        test3 = "test3";

        db = newRecordManager();


        BTree tree = BTree.createInstance(db);

        BTreeNode node = new BTreeNode(tree, test, test);

        BTree.BTreeTupleBrowser browser;
        BTree.BTreeTuple tuple = new BTree.BTreeTuple();

        // test insertion
        node.insert(1, test2, test2, false);
        node.insert(1, test3, test3, false);
        node.insert(1, test1, test1, false);

        // test binary search
        browser = node.find(1, test2,true);
        if (browser.getNext(tuple) == false) {
            throw new IllegalStateException("Browser didn't have 'test2'");
        }
        if (!tuple.key.equals(test2)) {
            throw new IllegalStateException("Tuple key is not 'test2'");
        }
        if (!tuple.value.equals(test2)) {
            throw new IllegalStateException("Tuple value is not 'test2'");
        }

        db.close();
        db = null;
    }


}
