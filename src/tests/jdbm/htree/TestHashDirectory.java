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

package jdbm.htree;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

import jdbm.RecordManager;
import jdbm.recman.TestCaseWithTestFile;
import junit.framework.TestSuite;

/**
 *  This class contains all Unit tests for {@link HashDirectory}.
 *
 *  @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 *  @version $Id: TestHashDirectory.java,v 1.2 2003/09/21 15:49:02 boisvert Exp $
 */
public class TestHashDirectory extends TestCaseWithTestFile {



    /**
     *  Basic tests
     */
    public void testBasics() throws IOException {
        System.out.println("testBasics");

        RecordManager recman = newRecordManager();

        HTree tree = new HTree();
        HashDirectory dir = new HashDirectory(tree, (byte)0);
        long recid = recman.insert(dir,tree.SERIALIZER);
        dir.setPersistenceContext(recman, recid);

        dir.put("key", "value");
        String s = (String)dir.get("key");
        assertEquals("value", s);

        recman.close();
    }

    /**
     *  Mixed tests
     */
    public void testMixed() throws IOException {
        System.out.println("testMixed");

        RecordManager recman = newRecordManager();
        HTree tree = new HTree();
        HashDirectory dir = new HashDirectory(tree, (byte)0);
        long recid = recman.insert(dir,tree.SERIALIZER);
        dir.setPersistenceContext(recman, recid);

        Hashtable hash = new Hashtable(); // use to compare results

        int max = 30; // must be even

        // insert & check values
        for (int i=0; i<max; i++) {
            dir.put("key"+i, "value"+i);
            hash.put("key"+i, "value"+i);
        }
        recman.commit();

        for (int i=0; i<max; i++) {
            String s = (String)dir.get("key"+i);
            assertEquals("value"+i, s);
        }
        recman.commit();

        // replace only even values
        for (int i=0; i<max; i+=2) {
            dir.put("key"+i, "value"+(i*2+1));
            hash.put("key"+i, "value"+(i*2+1));
        }
        recman.commit();

        for (int i=0; i<max; i++) {
            if ((i%2) == 1) {
                // odd
                String s = (String)dir.get("key"+i);
                assertEquals("value"+i, s);
            } else {
                // even
                String s = (String)dir.get("key"+i);
                assertEquals("value"+(i*2+1), s);
            }
        }
        recman.commit();

        // remove odd numbers
        for (int i=1; i<max; i+=2) {
            dir.remove("key"+i);
            hash.remove("key"+i);
        }
        recman.commit();

        for (int i=0; i<max; i++) {
            if ((i%2) == 1) {
                // odd
                String s = (String)dir.get("key"+i);
                assertEquals(null, s);
            } else {
                // even
                String s = (String)dir.get("key"+i);
                assertEquals("value"+(i*2+1), s);
            }
        }
        recman.commit();

        recman.close();
        recman = null;
    }

    void checkEnumerations(Hashtable hash, HashDirectory dir)
    throws IOException {

        // test keys
        Hashtable clone = (Hashtable) hash.clone();
        int count = 0;
        Iterator<String> iter = dir.keys();
         
        while ( iter.hasNext()) {
        	String s = iter.next();
            count++;
            clone.remove( s );
        }
        assertEquals(hash.size(), count);

        // test values
        clone = (Hashtable)hash.clone();
        count = 0;
        iter = dir.values();
        while (iter.hasNext()) {
        	String s = iter.next();
            count++;
            clone.remove( s );
        }
        assertEquals(hash.size(), count);
    }

    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(new TestSuite(TestHashDirectory.class));
    }

}
