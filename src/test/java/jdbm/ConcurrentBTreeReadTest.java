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
package jdbm;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;

public class ConcurrentBTreeReadTest extends TestCaseWithTestFile {
    

	public static class Dummy implements Serializable {
        
        private static final long serialVersionUID = -5567451291089724793L;
        private long key;
        @SuppressWarnings("unused")
        private byte space[] = new byte[1024];
        
	    public Dummy() {}
	    public Dummy(long key) { this.key = key; }
        
        @Override
        public int hashCode() {
            return (int)key;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Dummy))
                return false;
            Dummy other = (Dummy) obj;
            if (key != other.key)
                return false;
            return true;
        }
        
    }
    
    private RecordManager2 recman;
    
    private BTree btree;
    
    private int entries = 20000;

    private int readers = 5;
    
    public void setUp() throws Exception {
        super.setUp();
        recman = newRecordManager();
        btree = BTree.createInstance(recman, (Comparator) Collections.reverseOrder());
        System.err.println(recman.getClass());
    }  

    
    
    
    public void testConcurrent() throws Exception {
        Runnable read = new Runnable() {

            public void run() {
                read();
            }
            
        };
        Thread t[] = new Thread[readers];
        int c = 0;
        for (int i = 0; i < entries; i++) { 
            btree.insert((long)i, new Dummy(i), false);
            if (i % 1000 == 0) {
		        System.err.println("count " + i);
                commit();
            }
        }
        System.err.println("done!");
        commit();
        System.gc();
        Thread.sleep(1000);
        
        for (int i = 0; i < readers; i++) { t[c++] = new Thread(read); }
        
        System.err.println("start readers");
        long start = System.currentTimeMillis();
        for (int i = 0; i < t.length; i++) { t[i].start(); }
        for (int i = 0; i < t.length; i++) { t[i].join(); }
        long end = System.currentTimeMillis();
        System.err.println("done " + (end - start) + "ms");
    }
    
    private Object fetch(Long id) throws IOException {
        try {
	        return btree.get(id);
        } catch (IOException e) {
            System.out.println("ERR " + id);
            e.printStackTrace();
            return null;
        }
    }

    private void commit() throws IOException {
        recman.commit();
    }

    private void read() {
        Random r = new Random();
        for (int i = 0; i < entries; i++) {
            try {
                fetch((long)r.nextInt(entries));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        System.err.println("done read");
    }
    
}

 	  	 
