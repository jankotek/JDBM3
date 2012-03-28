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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;

/**
 * This class contains all Unit tests for {@link HTree}.
 *
 * @author Alex Boisvert
 */
public class HTreeTest extends TestCaseWithTestFile {


    /**
     * Basic tests
     */
    public void testIterator() throws IOException {

        DBAbstract db = newDBCache();

        HTree testTree = (HTree) db.createHashMap("tree");

        int total = 10;
        for (int i = 0; i < total; i++) {
            testTree.put(Long.valueOf("" + i), Long.valueOf("" + i));
        }
        db.commit();

        Iterator fi = testTree.values().iterator();
        Object item;
        int count = 0;
        while (fi.hasNext()) {
            fi.next();
            count++;
        }
        assertEquals(count, total);

        db.close();
    }

    public void testRecordListener() throws IOException {
        DBAbstract db = newDBCache();
        HTree<Integer, String> tree = (HTree) db.createHashMap("test");
        final List<SimpleEntry<Integer, String>> dels = new ArrayList();
        final List<SimpleEntry<Integer, String>> ins = new ArrayList();
        final List<SimpleEntry<Integer, String>> updNew = new ArrayList();
        final List<SimpleEntry<Integer, String>> updOld = new ArrayList();

        tree.addRecordListener(new RecordListener<Integer, String>() {

            public void recordUpdated(Integer key, String oldValue, String newValue) throws IOException {
                updOld.add(new SimpleEntry<Integer, String>(key, oldValue));
                updNew.add(new SimpleEntry<Integer, String>(key, newValue));
            }

            public void recordRemoved(Integer key, String value) throws IOException {
                dels.add(new SimpleEntry<Integer, String>(key, value));
            }

            public void recordInserted(Integer key, String value) throws IOException {
                ins.add(new SimpleEntry<Integer, String>(key, value));
            }
        });

        //test insert
        tree.put(11, "aa11");
        tree.put(12, "aa12");
        assertTrue(ins.contains(new SimpleEntry(11, "aa11")));
        assertTrue(ins.contains(new SimpleEntry(12, "aa12")));
        assertTrue(ins.size() == 2);
        ins.clear();
        assertTrue(dels.isEmpty());
        assertTrue(updNew.isEmpty());
        assertTrue(updOld.isEmpty());

        //test update
        tree.put(12, "aa123");
        assertTrue(ins.isEmpty());
        assertTrue(dels.isEmpty());
        assertTrue(updOld.contains(new SimpleEntry(12, "aa12")));
        assertTrue(updOld.size() == 1);
        updOld.clear();
        assertTrue(updNew.contains(new SimpleEntry(12, "aa123")));
        assertTrue(updNew.size() == 1);
        updNew.clear();

        //test remove
        tree.remove(11);
        assertTrue(dels.contains(new SimpleEntry(11, "aa11")));
        assertTrue(dels.size() == 1);
        dels.clear();
        assertTrue(ins.isEmpty());
        assertTrue(updOld.isEmpty());
        assertTrue(updNew.isEmpty());

    }

    public void testIssue(){
        int size = 100000;
        int commitSize = 100000;
        DB build = DBMaker.openFile(newTestFile()).setMRUCacheSize(100).make();
        Map<String, String> hashMap = build.createHashMap("hashMap");
        for (int i = 0; i < size; i++) {
            hashMap.put(i + "asdddfdgf" + i + "sddfdfsf" + i, "dsfgfg.dfcdfsgfg");
            if (i % commitSize == 0) {
                build.commit();
            }
        }
        build.commit();
        build.calculateStatistics();
        build.close();
    }


}
