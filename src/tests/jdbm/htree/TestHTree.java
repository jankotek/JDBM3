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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;

import jdbm.PrimaryHashMap;
import jdbm.RecordListener;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.recman.TestCaseWithTestFile;
import junit.framework.TestSuite;

/**
 *  This class contains all Unit tests for {@link HTree}.
 *
 *  @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 *  @version $Id: TestHTree.java,v 1.2 2003/09/21 15:49:02 boisvert Exp $
 */
public class TestHTree extends TestCaseWithTestFile {



    /**
     *  Basic tests
     */
    public void testIterator() throws IOException {
        
        RecordManager recman = newRecordManager();

        HTree testTree = getHtree(recman, "htree");
    
        int total = 10;
        for ( int i = 0; i < total; i++ ) {
            testTree.put( Long.valueOf("" + i), Long.valueOf("" + i) );
        }
        recman.commit();
    
        Iterator fi = testTree.values();
        Object item;
        int count = 0;
        while(fi.hasNext()) {
        	fi.next();
            count++;
        }
        assertEquals( count, total );

        recman.close();
    }

    public void testRecordListener() throws IOException{
        RecordManager recman = RecordManagerFactory.createRecordManager( "test" );
        HTree<Integer,String> tree = HTree.createInstance( recman);
        final List<SimpleEntry<Integer,String>> dels = new ArrayList();
        final List<SimpleEntry<Integer,String>> ins = new ArrayList();
        final List<SimpleEntry<Integer,String>> updNew = new ArrayList();
        final List<SimpleEntry<Integer,String>> updOld = new ArrayList();
        
        tree.addRecordListener(new RecordListener<Integer, String>() {
			
			public void recordUpdated(Integer key, String oldValue, String newValue) throws IOException {
				updOld.add(new SimpleEntry<Integer, String>(key,oldValue));
				updNew.add(new SimpleEntry<Integer, String>(key,newValue));
			}
			
			public void recordRemoved(Integer key, String value) throws IOException {
				dels.add(new SimpleEntry<Integer, String>(key,value));
			}
			
			public void recordInserted(Integer key, String value) throws IOException {
				ins.add(new SimpleEntry<Integer, String>(key,value));				
			}
		});
        
        //test insert
        tree.put(11, "aa11");
        tree.put(12, "aa12");
        assertTrue(ins.contains(new SimpleEntry(11,"aa11")));
        assertTrue(ins.contains(new SimpleEntry(12,"aa12")));
        assertTrue(ins.size() == 2);
        ins.clear();
        assertTrue(dels.isEmpty());
        assertTrue(updNew.isEmpty());
        assertTrue(updOld.isEmpty());
        
        //test update
        tree.put(12, "aa123");
        assertTrue(ins.isEmpty());
        assertTrue(dels.isEmpty());
        assertTrue(updOld.contains(new SimpleEntry(12,"aa12")));
        assertTrue(updOld.size() == 1);
        updOld.clear();
        assertTrue(updNew.contains(new SimpleEntry(12,"aa123")));
        assertTrue(updNew.size() == 1);
        updNew.clear();
        
        //test remove
        tree.remove(11);
        assertTrue(dels.contains(new SimpleEntry(11,"aa11")));
        assertTrue(dels.size() == 1);
        dels.clear();
        assertTrue(ins.isEmpty());
        assertTrue(updOld.isEmpty());
        assertTrue(updNew.isEmpty());

    }

    
    private static HTree getHtree( RecordManager recman, String name )
      throws IOException
    {
        long recId = recman.getNamedObject("htree");  
        HTree testTree;
        if ( recId != 0 ) {
            testTree = HTree.load( recman, recId );
        } else {
            testTree = HTree.createInstance( recman );
            recman.setNamedObject( "htree", testTree.getRecid() );
        }
        return testTree;
    }


    /**
     *  Runs all tests in this class
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(new TestSuite(TestHTree.class));
    }

}
