/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 */

package jdbm.htree;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

import jdbm.RecordManager;
import jdbm.recman._TestCaseWithTestFile;
import junit.framework.TestSuite;

/**
 *  This class contains all Unit tests for {@link HashDirectory}.
 *
 *  @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 *  @version $Id: TestHashDirectory.java,v 1.2 2003/09/21 15:49:02 boisvert Exp $
 */
public class TestHashDirectory extends _TestCaseWithTestFile {



    /**
     *  Basic tests
     */
    public void testBasics() throws IOException {
        System.out.println("testBasics");

        RecordManager recman = newRecordManager();

        HashDirectory dir = new HashDirectory((byte)0);
        long recid = recman.insert(dir,HashNode.SERIALIZER);
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
        HashDirectory dir = new HashDirectory((byte)0);
        long recid = recman.insert(dir,HashNode.SERIALIZER);
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
