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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;

import jdbm.PrimaryStoreMap;
import jdbm.RecordListener;
import jdbm.RecordManager;
import jdbm.SecondaryHashMap;
import jdbm.SecondaryKeyExtractor;
import jdbm.SecondaryTreeMap;
import jdbm.recman.TestCaseWithTestFile;

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
        RecordManager recman = newRecordManager();
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
    
    @SuppressWarnings("unchecked")
	public void testStoreMapSecondaryTreeListeners() throws IOException{
    	RecordManager recman = newRecordManager();
    	PrimaryStoreMap<Long,String> st = recman.storeMap("storeMap");
    	SecondaryKeyExtractor<String, Long, String> extractor = new SecondaryKeyExtractor<String, Long, String>() {
			public String extractSecondaryKey(Long key, String value) {				
				return ""+key+value;
			}};
    	SecondaryTreeMap t = st.secondaryTreeMap("map1",extractor); 
    	SecondaryHashMap h = st.secondaryHashMap("map2",extractor);
    	Long key = st.putValue("aaa");
    	assertTrue(t.size() == 1);
    	assertTrue(t.containsKey(""+key+"aaa"));
    	assertTrue(h.size() == 1);
    	assertTrue(h.containsKey(""+key+"aaa"));
    	
    	//defrag will force reopening
    	recman.defrag();
    	recman.clearCache();
    	
    	assertTrue(t.size() == 1);
    	assertTrue(t.containsKey(""+key+"aaa"));
    	assertTrue(h.size() == 1);
    	assertTrue(h.containsKey(""+key+"aaa"));

		

    }


}
