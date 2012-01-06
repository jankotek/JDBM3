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

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

/**
 * Subclass from this class if you have any test cases that need to do file I/O. The
 * setUp() and tearDown() methods here will take care of cleanup on disk.
 *
 * @author cdegroot <cg@cdegroot.com>
 */
abstract class TestCaseWithTestFile extends TestCase {

    public static final String testFolder = System.getProperty("java.io.tmpdir", ".") + "/_testdb";
//	public static final String testFileName = "test";


    public void setUp() throws Exception {
        File f = new File(testFolder);
        if (!f.exists())
            f.mkdirs();
    }

    public void tearDown() throws Exception {
        File f = new File(testFolder);
        if (f.exists()) {
            for (File f2 : f.listFiles()) {
                f2.deleteOnExit();
                f2.delete();
            }
        }
    }

    static public String newTestFile() {
        return testFolder + File.separator + "test" + System.nanoTime();
    }

    static public RecordFile newRecordFile() throws IOException {
        return new RecordFile(newTestFile());
    }

    static public DBAbstract newRecordManager() throws IOException {
        return (DBAbstract) new DBMaker(newTestFile()).build();
    }

    static public DBStore newBaseRecordManager() throws IOException {
        return (DBStore) new DBMaker(newTestFile()).disableCache().build();
    }


}
