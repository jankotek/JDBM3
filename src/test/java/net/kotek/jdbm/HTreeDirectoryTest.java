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
import java.util.Hashtable;
import java.util.Iterator;

/**
 * This class contains all Unit tests for {@link HTreeDirectory}.
 *
 * @author Alex Boisvert
 */
public class HTreeDirectoryTest extends TestCaseWithTestFile {


    /**
     * Basic tests
     */
    public void testBasics() throws IOException {
        System.out.println("testBasics");

        DBAbstract db = newDBCache();

        HTree tree = (HTree) db.createHashMap("test");
        HTreeDirectory dir = tree.getRoot();

        dir.put("key", "value");
        String s = (String) dir.get("key");
        assertEquals("value", s);

        db.close();
    }

    /**
     * Mixed tests
     */
    public void testMixed() throws IOException {
        System.out.println("testMixed");

        DBAbstract db = newDBCache();

        HTree tree = (HTree) db.createHashMap("test");
        HTreeDirectory dir = tree.getRoot();

        Hashtable hash = new Hashtable(); // use to compare results

        int max = 30; // must be even

        // insert & check values
        for (int i = 0; i < max; i++) {
            dir.put("key" + i, "value" + i);
            hash.put("key" + i, "value" + i);
        }
        db.commit();

        for (int i = 0; i < max; i++) {
            String s = (String) dir.get("key" + i);
            assertEquals("value" + i, s);
        }
        db.commit();

        // replace only even values
        for (int i = 0; i < max; i += 2) {
            dir.put("key" + i, "value" + (i * 2 + 1));
            hash.put("key" + i, "value" + (i * 2 + 1));
        }
        db.commit();

        for (int i = 0; i < max; i++) {
            if ((i % 2) == 1) {
                // odd
                String s = (String) dir.get("key" + i);
                assertEquals("value" + i, s);
            } else {
                // even
                String s = (String) dir.get("key" + i);
                assertEquals("value" + (i * 2 + 1), s);
            }
        }
        db.commit();

        // remove odd numbers
        for (int i = 1; i < max; i += 2) {
            dir.remove("key" + i);
            hash.remove("key" + i);
        }
        db.commit();

        for (int i = 0; i < max; i++) {
            if ((i % 2) == 1) {
                // odd
                String s = (String) dir.get("key" + i);
                assertEquals(null, s);
            } else {
                // even
                String s = (String) dir.get("key" + i);
                assertEquals("value" + (i * 2 + 1), s);
            }
        }
        db.commit();

        db.close();
        db = null;
    }

    void checkEnumerations(Hashtable hash, HTreeDirectory dir)
            throws IOException {

        // test keys
        Hashtable clone = (Hashtable) hash.clone();
        int count = 0;
        Iterator<String> iter = dir.keys();

        while (iter.hasNext()) {
            String s = iter.next();
            count++;
            clone.remove(s);
        }
        assertEquals(hash.size(), count);

        // test values
        clone = (Hashtable) hash.clone();
        count = 0;
        iter = dir.values();
        while (iter.hasNext()) {
            String s = iter.next();
            count++;
            clone.remove(s);
        }
        assertEquals(hash.size(), count);
    }


}
