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

import java.io.IOError;
import java.io.IOException;

public class FileLockTest extends TestCaseWithTestFile {

    public void testLock() throws IOException {
        String file = newTestFile();

        DB db1 = DBMaker.openFile(file).make();
        //now open same file second time, exception should be thrown
        try {
            DB db2 = DBMaker.openFile(file).make();
            fail("Exception should be thrown if file was locked");
        } catch (IOError e) {
            //expected
        }

        db1.close();

        //after close lock should be released, reopen
        DB db3 = DBMaker.openFile(file).make();
        db3.close();
    }
}
