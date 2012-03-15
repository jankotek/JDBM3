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

/**
 * Test cases for HTree rollback
 */
public class TestRollback
        extends TestCaseWithTestFile {


    /**
     * Test case courtesy of Derek Dick (mailto:ddick  users.sourceforge.net)
     */
    public void testRollback1()
            throws Exception {

        // Note: We start out with an empty file
        DBAbstract db = newDBCache();

        HTree tree = (HTree) db.createHashMap("test");

        tree.put("Foo", "Bar");
        tree.put("Fo", "Fum");

        db.commit();

        tree.put("Hello", "World");

        db.rollback();

        assertTrue(tree.get("Foo").equals("Bar"));
        assertTrue(tree.get("Fo").equals("Fum"));
        assertTrue(tree.get("Hello") == null);
    }


    /**
     * Test case courtesy of Derek Dick (mailto:ddick  users.sourceforge.net)
     */
    public void testRollback2()
            throws Exception {

        DBAbstract db = newDBCache();

        HTree tree = (HTree) db.createHashMap("test");

        tree.put("hello", "world");
        tree.put("goodnight", "gracie");
        db.commit();

        tree.put("derek", "dick");
        db.rollback();

        assertTrue(tree.get("derek") == null);
        assertTrue(tree.get("goodnight").equals("gracie"));
        assertTrue(tree.get("hello").equals("world"));
    }


    /**
     * Test case courtesy of Derek Dick (mailto:ddick  users.sourceforge.net)
     */
    public void testRollback1b()
            throws Exception {

        // Note: We start out with an empty file
        DBAbstract db = newDBCache();

        HTree<Object, Object> tree = (HTree<Object, Object>) db.createHashMap("test");

        tree.put("Foo", "Bar");
        tree.put("Fo", "Fum");

        db.commit();

        tree.put("Hello", "World");

        db.rollback();

        assertTrue(tree.get("Foo").equals("Bar"));
        assertTrue(tree.get("Fo").equals("Fum"));
        assertTrue(tree.get("Hello") == null);
    }


    /**
     * Test case courtesy of Derek Dick (mailto:ddick  users.sourceforge.net)
     */
    public void testRollback2b()
            throws Exception {
        DBAbstract db;
        long root;

        // Note: We start out with an empty file
        db = newDBCache();

        root = db.getNamedObject("xyz");

        BTree tree = null;
        if (root == 0) {
            // create a new one
            tree = BTree.createInstance(db);
            root = tree.getRecid();
            db.setNamedObject("xyz", root);
            db.commit();
        } else {
            tree = BTree.load(db, root);
        }

        tree.insert("hello", "world", true);
        tree.insert("goodnight", "gracie", true);
        db.commit();

        tree.insert("derek", "dick", true);
        db.rollback();

        assertTrue(tree.get("derek") == null);
        assertTrue(tree.get("goodnight").equals("gracie"));
        assertTrue(tree.get("hello").equals("world"));
    }


}
