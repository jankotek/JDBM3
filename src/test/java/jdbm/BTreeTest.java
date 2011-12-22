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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.locks.Lock;

import junit.framework.AssertionFailedError;
import junit.framework.TestResult;

/**
 *  This class contains all Unit tests for {@link BTree}.
 *
 *  @author Alex Boisvert
 */
public class BTreeTest
    extends TestCaseWithTestFile
{

      static final boolean DEBUG = false;

      // the number of threads to be started in the synchronization test
      static final int THREAD_NUMBER = 5;

      // the size of the content of the maps for the synchronization
      // test. Beware that THREAD_NUMBER * THREAD_CONTENT_COUNT < Integer.MAX_VALUE.
      static final int THREAD_CONTENT_SIZE = 150;

      // for how long should the threads run.
      static final int THREAD_RUNTIME = 10 * 1000;

    protected TestResult result_;

    /**
     * Overrides TestCase.run(TestResult), so the errors from threads
     * started from this thread can be added to the testresult. This is
     * shown in
     * http://www.javaworld.com/javaworld/jw-12-2000/jw-1221-junit.html
     *
     * @param result the testresult
     */
    public void run( TestResult result )
    {
        result_ = result;
        super.run( result );
        result_ = null;
    }

//----------------------------------------------------------------------
/**
 * Handles the exceptions from other threads, so they are not ignored
 * in the junit test result. This method must be called from every
 * thread's run() method, if any throwables were throws.
 *
 * @param t the throwable (either from an assertEquals, assertTrue,
 * fail, ... method, or an uncaught exception to be added to the test
 * result of the junit test.
 */

  protected void handleThreadException(final Throwable t)
  {
    synchronized(result_)
    {
      if(t instanceof AssertionFailedError)
        result_.addFailure(this,(AssertionFailedError)t);
      else
        result_.addError(this,t);
    }
  }


    /**
     *  Basic tests
     */
    public void testBasics() throws IOException {
        DBAbstract db;
        BTree          tree;
        byte[]         test, test0, test1, test2, test3;
        byte[]         value1, value2;

        test  = "test".getBytes();
        test0 = "test0".getBytes();
        test1 = "test1".getBytes();
        test2 = "test2".getBytes();
        test3 = "test3".getBytes();

        value1 = "value1".getBytes();
        value2 = "value2".getBytes();

        if ( DEBUG ) {
            System.out.println("BTreeTest.testBasics");
        }

        db = newRecordManager();
        tree = BTree.createInstance( db, new ByteArrayComparator() );

        tree.insert( test1, value1, false );
        tree.insert( test2, value2, false );

        byte[] result;
        result = (byte[]) tree.get(test0);
        if ( result != null ) {
            throw new Error( "Test0 shouldn't be found" );
        }

        result = (byte[]) tree.get(test1);
        if ( result == null || ByteArrayComparator.compareByteArray( result, value1 ) != 0 ) {
            throw new Error( "Invalid value for test1: " + result );
        }

        result = (byte[]) tree.get(test2);
        if ( result == null || ByteArrayComparator.compareByteArray( result, value2 ) != 0 ) {
            throw new Error( "Invalid value for test2: " + result );
        }

        result = (byte[]) tree.get(test3);
        if ( result != null ) {
            throw new Error( "Test3 shouldn't be found" );
        }

        db.close();
    }

    /**
     *  Basic tests, just use the simple test possibilities of junit (cdaller)
     */
    public void testBasics2() throws IOException {
        DBAbstract db;
        BTree          tree;
        byte[]         test, test0, test1, test2, test3;
        byte[]         value1, value2;

        test  = "test".getBytes();
        test0 = "test0".getBytes();
        test1 = "test1".getBytes();
        test2 = "test2".getBytes();
        test3 = "test3".getBytes();

        value1 = "value1".getBytes();
        value2 = "value2".getBytes();

        if ( DEBUG )
            System.out.println("BTreeTest.testBasics2");

        db = newRecordManager();
        tree = BTree.createInstance( db, new ByteArrayComparator() );

        tree.insert( test1, value1, false);
        tree.insert( test2, value2, false);

        assertEquals( null, tree.get(test0) );
        assertEquals( 0, ByteArrayComparator.compareByteArray( value1, (byte[]) tree.get(test1) ) );
        assertEquals( 0, ByteArrayComparator.compareByteArray( value2, (byte[]) tree.get(test2) ) );
        assertEquals( null, (byte[]) tree.get(test3) );

        db.close();
     }


    /**
     *  Test what happens after the dbager has been closed but the
     *  btree is accessed. WHAT SHOULD HAPPEN???????????
     * (cdaller)
     */
    public void testClose()
        throws IOException
    {
        DBAbstract db;
        BTree          tree;
        byte[]         test, test0, test1, test2, test3;
        byte[]         value1, value2;

        test  = "test".getBytes();
        test0 = "test0".getBytes();
        test1 = "test1".getBytes();
        test2 = "test2".getBytes();
        test3 = "test3".getBytes();

        value1 = "value1".getBytes();
        value2 = "value2".getBytes();

        if ( DEBUG )
            System.out.println("BTreeTest.testClose");

        db = newRecordManager();
        tree = BTree.createInstance( db, new ByteArrayComparator() );

        tree.insert( test1, value1, false );
        tree.insert( test2, value2, false );

        assertEquals( null, tree.get(test0) );
        assertEquals( 0, ByteArrayComparator.compareByteArray( value1, (byte[]) tree.get(test1) ) );
        assertEquals( 0, ByteArrayComparator.compareByteArray(value2, (byte[]) tree.get(test2)) );
        assertEquals( null, (byte[]) tree.get(test3) );

        db.close();

        try {
            tree.browse();
            fail("Should throw an IllegalStateException on access on not opened btree");
        } catch( IllegalStateException except ) {
            // ignore
        }

        try {
            tree.get(test0);
            fail( "Should throw an IllegalStateException on access on not opened btree" );
        } catch( IllegalStateException except ) {
            // ignore
        }

        try {
            tree.findGreaterOrEqual( test0 );
            fail( "Should throw an IllegalStateException on access on not opened btree" );
        } catch( IllegalStateException except ) {
            // ignore
        }

        try {
            tree.insert( test2, value2, false );
            fail( "Should throw an IllegalStateException on access on not opened btree" );
        } catch( IllegalStateException except ) {
            // ignore
        }

        try {
            tree.remove( test0 );
            fail( "Should throw an IllegalStateException on access on not opened btree" );
        } catch( IllegalStateException except ) {
            // ignore
        }

        /*
        try {
            tree.size();
            fail( "Should throw an IllegalStateException on access on not opened btree" );
        } catch( IllegalStateException except ) {
            // ignore
        }
        */
     }


    /**
     *  Test to insert different objects into one btree. (cdaller)
     */
    public void testInsert()
        throws IOException
    {
        DBAbstract db;
        BTree          tree;

        if ( DEBUG )
            System.out.println("BTreeTest.testInsert");

        db = newRecordManager();
        tree = BTree.createInstance( db);

        // insert differnt objects and retrieve them
        tree.insert("test1", "value1",false);
        tree.insert("test2","value2",false);
        tree.insert("one", new Integer(1),false);
        tree.insert("two",new Long(2),false);
        tree.insert("myownobject",new ObjectTT(new Integer(234)),false);

        assertEquals("value2",(String)tree.get("test2"));
        assertEquals("value1",(String)tree.get("test1"));
        assertEquals(new Integer(1),(Integer)tree.get("one"));
        assertEquals(new Long(2),(Long)tree.get("two"));

        // what happens here? must not be replaced, does it return anything?
        // probably yes!
        assertEquals("value1",tree.insert("test1","value11",false));
        assertEquals("value1",tree.get("test1")); // still the old value?
        assertEquals("value1",tree.insert("test1","value11",true));
        assertEquals("value11",tree.get("test1")); // now the new value!

        ObjectTT expected_obj = new ObjectTT(new Integer(234));
        ObjectTT btree_obj = (ObjectTT)tree.get("myownobject");
        assertEquals(expected_obj, btree_obj);

        db.close();
    }


    /**
     *  Test to remove  objects from the btree. (cdaller)
     */
    public void testRemove()
        throws IOException
    {
        DBAbstract db;
        BTree          tree;

        if ( DEBUG ) {
            System.out.println( "BTreeTest.testRemove" );
        }

        db = newRecordManager();
        tree = BTree.createInstance( db);

        tree.insert("test1", "value1",false);
        tree.insert("test2","value2",false);
        assertEquals("value1",(String)tree.get("test1"));
        assertEquals("value2",(String)tree.get("test2"));
        tree.remove("test1");
        assertEquals(null,(String)tree.get("test1"));
        assertEquals("value2",(String)tree.get("test2"));
        tree.remove("test2");
        assertEquals(null,(String)tree.get("test2"));

        int iterations = 1000;

        for ( int count = 0; count < iterations; count++ ) {
            tree.insert( "num"+count, new Integer( count ), false );
        }

        assertEquals( iterations, tree.size() );

        for ( int count = 0; count < iterations; count++ ) {
            assertEquals( new Integer( count ), tree.get("num" + count) );
        }

        for ( int count = 0; count < iterations; count++ ) {
           tree.remove( "num" + count );
        }

        assertEquals( 0, tree.size() );

        db.close();
    }

    /**
     *  Test to get differents objects in the btree. (cdaller)
     */
    public void testFind()
        throws IOException
    {
        DBAbstract db;
        BTree          tree;

        if ( DEBUG )
            System.out.println("BTreeTest.testFind");

        db = newRecordManager();
        tree = BTree.createInstance( db);

        tree.insert("test1", "value1",false);
        tree.insert("test2","value2",false);

        Object value = tree.get("test1");
        assertTrue(value instanceof String);
        assertEquals("value1",value);

        tree.insert("","Empty String as key",false);
        assertEquals("Empty String as key",(String)tree.get(""));

        assertEquals(null,(String)tree.get("someoneelse"));

        db.close();
    }
    /**
     *  Test deletion of btree from record manager. (kday)
     *  
     *  After deletion, the BTree and all of it's BTreeNode children (and their children)
     *  should be removed from the recordmanager.
     */
    public void testDelete()
        throws IOException
    {
        if ( DEBUG )
            System.out.println("BTreeTest.testFind");
        DBAbstract db = newRecordManager();
        BTree<String, Serializable> tree = BTree.createInstance( db);

        // put enough data into the tree so we definitely have multiple nodes
        for (int count = 1; count <= 1000; count++){
            tree.insert("num" + count,new Integer(count),false);
            if (count % 100 == 0)
                db.commit();
        }
        List<Long> out = new ArrayList<Long>();
        tree.dumpChildNodeRecIDs(out);
        assertTrue(out.size() > 0);
    }


    /**
     *  Test to insert, retrieve and remove a large amount of data. (cdaller)
     */
    public void testLargeDataAmount()
        throws IOException
    {

        DBAbstract db;
        BTree          tree;

        if ( DEBUG )
            System.out.println("BTreeTest.testLargeDataAmount");

        db = newRecordManager();
        // db = new jdbm.db.BaseRecordManager( "test" );
        
        tree = BTree.createInstance( db);
        // tree.setSplitPoint( 4 );

        int iterations = 10000;

        // insert data
        for ( int count = 0; count < iterations; count++ ) {
           try {
            assertEquals(null,tree.insert("num"+count,new Integer(count),false));
           } catch ( IOException except ) {
               except.printStackTrace();
               throw except;
           }
        }

           // get data
         for(int count = 0; count < iterations; count++)
         {
           assertEquals(new Integer(count), tree.get("num" + count));
         }

             // delete data
         for(int count = 0; count < iterations; count++)
         {
           assertEquals(new Integer(count),tree.remove("num"+count));
         }

         assertEquals(0,tree.size());

         db.close();
   }
    
    public void testRecordListener() throws IOException{
        DBAbstract db = newRecordManager();
        BTree<Integer,String> tree = BTree.createInstance( db);
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
        tree.insert(11, "aa11", true);
        tree.insert(12, "aa12", true);
        assertTrue(ins.contains(new SimpleEntry(11,"aa11")));
        assertTrue(ins.contains(new SimpleEntry(12,"aa12")));
        assertTrue(ins.size() == 2);
        ins.clear();
        assertTrue(dels.isEmpty());
        assertTrue(updNew.isEmpty());
        assertTrue(updOld.isEmpty());
        
        //test update
        tree.insert(12, "aa123", true);
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


    /**
     * Tests the corner case of deleting all nodes from the tree.  In this case, all BTreeNodes
     * associated with the tree should be removed from the db.
     * 
     * We are also going to test to make sure the db file doesn't grow (leak) if we repeat the
     * process a number of times.
     * @throws Exception
     */
    public void testDeleteAllNodes() throws Exception {

        
        // we are going to run this test without object cache enabled.  If it is turned on,
        // we will have problems with using a different deserializer for BTreeNodes than the standard
        // serializer.

        String recordManagerBasename = newTestFile();
        String recordManagerDBname = recordManagerBasename+".d.0";
        
        long previousdbSize = 0;
        for (int i = 0; i < 5; i++){
            DBAbstract db = (DBAbstract) new DBMaker(recordManagerBasename).disableCache().build();
      
            try{
                BTree<String, Serializable> tree = BTree.createInstance( db);
                String[] keys = new String[1000];
                for (int count = 0; count < 1000; count++){
                    keys[count] = "num" + count;
                }      
          
                // put enough data into the tree so we definitely have multiple nodes
                for (int count = 0; count < 1000; count++){
                    tree.insert(keys[count],new Integer(count),false);
                    if (count % 100 == 0)
                        db.commit();
                }
                db.commit();
                
                long currentdbSize = new File(recordManagerDBname).length();
                assertTrue("file size too small "+ currentdbSize, currentdbSize > 0);

                
                // now remove it all
                for (int count = 0; count < 1000; count++){
                    tree.remove(keys[count]);
                    if (count % 100 == 0)
                        db.commit();
                }
                db.commit();
                
                BTreeNode root = tree.getRoot();
                assertNull(root);
                
            } finally {
                db.close();
                long currentdbSize = new File(recordManagerDBname).length();
                assertTrue("file size too small "+ currentdbSize, currentdbSize > 0);
                if (previousdbSize != 0){
                    assertTrue(currentdbSize == previousdbSize);
                }
            }
        }
    }

    
/**
 * Test access from multiple threads. Assertions only work, when the
 * run() method is overridden and the exceptions of the threads are
 * added to the resultset of the TestCase. see run() and
 * handleException().
 */
  public void testMultithreadAccess()
    throws IOException
  {
        DBAbstract db;
        BTree          tree;

        if ( DEBUG )
            System.out.println("BTreeTest.testMultithreadAccess");

        db = newRecordManager();
        tree = BTree.createInstance( db);

        TestThread[] thread_pool = new TestThread[THREAD_NUMBER];
        String name;
        Map content;

        // create content for the tree, different content for different threads!
        for (int thread_count = 0; thread_count < THREAD_NUMBER; thread_count++) {
            name = "thread"+thread_count;
            content = new TreeMap();
            for(int content_count = 0; content_count < THREAD_CONTENT_SIZE; content_count++) {
                // guarantee, that keys and values do not overleap,
                // otherwise one thread removes some keys/values of
                // other threads!
                content.put( name+"_"+content_count,
                             new Integer(thread_count*THREAD_CONTENT_SIZE+content_count) );
            }
            thread_pool[thread_count] = new TestThread(name,tree,content);
            thread_pool[thread_count].start();
        }

        try {
            Thread.sleep(THREAD_RUNTIME);
        } catch( InterruptedException ignore ) {
            ignore.printStackTrace();
        }

        // stop threads:
        for (int thread_count = 0; thread_count < THREAD_NUMBER; thread_count++) {
            if ( DEBUG ) System.out.println("Stop threads");
            thread_pool[thread_count].setStop();
        }
        // wait until the threads really stop:
        try {
            for (int thread_count = 0; thread_count < THREAD_NUMBER; thread_count++) {
                if ( DEBUG ) System.out.println("Join thread " + thread_count );
                thread_pool[thread_count].join();
                if ( DEBUG ) System.out.println("Joined thread " + thread_count );
            }
        } catch( InterruptedException ignore ) {
            ignore.printStackTrace();
        }
        db.close();
    }


  
    /**
     *  Helper method to 'simulate' the methods of an entry set of the btree.
     */
    protected static boolean containsKey( Object key, BTree btree )
        throws IOException
    {
        return ( btree.get(key) != null );
    }


    /**
     *  Helper method to 'simulate' the methods of an entry set of the btree.
     */
    protected static boolean containsValue( Object value, BTree btree )
        throws IOException
  {
    // we must synchronize on the BTree while browsing
    	Lock readLock = btree.getLock().readLock();
    	try {
    		readLock.lock();
	        BTree.BTreeTupleBrowser browser = btree.browse();
	        BTree.BTreeTuple tuple = new BTree.BTreeTuple();
	        while(browser.getNext(tuple)) {
	          if(tuple.value.equals(value))
	            return(true);
	        }
    	} finally {
    		readLock.unlock();
    	}
    //    System.out.println("Comparation of '"+value+"' with '"+ tuple.getValue()+"' FAILED");
    return(false);
  }

    /**
     *  Helper method to 'simulate' the methods of an entry set of the btree.
     */
    protected static boolean contains( Map.Entry entry, BTree btree )
        throws IOException
    {
        Object tree_obj = btree.get(entry.getKey());
        if ( tree_obj == null ) {
            // can't distuingish, if value is null or not found!!!!!!
            return ( entry.getValue() == null );
        }
        return ( tree_obj.equals( entry.getValue() ) );
    }


    /**
     * Inner class for testing puroposes only (multithreaded access)
     */
    class TestThread
        extends Thread
    {
        Map _content;
        BTree _btree;
        volatile boolean _continue = true;
        int THREAD_SLEEP_TIME = 50; // in ms
        String _name;

        TestThread( String name, BTree btree, Map content )
        {
            _content = content;
            _btree = btree;
            _name = name;
        }

        public void setStop()
        {
            _continue = false;
        }

        private void action()
            throws IOException
        {
            Iterator iterator = _content.entrySet().iterator();
            Map.Entry entry;
            if ( DEBUG ) {
                System.out.println("Thread "+_name+": fill btree.");
            }
            while( iterator.hasNext() ) {
                entry = (Map.Entry) iterator.next();
                assertEquals( null, _btree.insert( entry.getKey(), entry.getValue(), false ) );
            }

            // as other threads are filling the btree as well, the size
            // of the btree is unknown (but must be at least the size of
            // the content map)
            assertTrue( _content.size() <= _btree.size() );

            iterator = _content.entrySet().iterator();
            if ( DEBUG ) {
                System.out.println( "Thread " + _name + ": iterates btree." );
            }
            while( iterator.hasNext() ) {
                entry = (Map.Entry) iterator.next();
                assertEquals( entry.getValue(), _btree.get(entry.getKey()) );
                assertTrue( contains( entry, _btree ) );
                assertTrue( containsKey( entry.getKey(), _btree ) );
                assertTrue( containsValue( entry.getValue(), _btree ) );
            }

            iterator = _content.entrySet().iterator();
            Object key;
            if ( DEBUG ) {
                System.out.println( "Thread " + _name + ": removes his elements from the btree." );
            }
            while( iterator.hasNext() ) {
                key = ( (Map.Entry) iterator.next() ).getKey();
                _btree.remove( key );
                assertTrue( ! containsKey( key,_btree ) );
            }
        }

        public void run()
        {
          if(DEBUG)
            System.out.println("Thread "+_name+": started.");
          try {
            while(_continue) {
              action();
              try {
                Thread.sleep(THREAD_SLEEP_TIME);
              } catch( InterruptedException except ) {
                except.printStackTrace();
              }
            }
          } catch( Throwable t ) {
            if ( DEBUG ) {
              System.err.println("Thread "+_name+" threw an exception:");
              t.printStackTrace();
            }
            handleThreadException(t);
          }
          if(DEBUG)
            System.out.println("Thread "+_name+": stopped.");
        }
      } // end of class TestThread

static class ObjectTT
    implements Serializable
{

    Object _content;

    private ObjectTT()
    {
        // empty
    }


    public ObjectTT(Object content)
    {
        _content = content;
    }


    Object getContent()
    {
        return _content;
    }


    public boolean equals( Object obj )
    {
        if ( ! ( obj instanceof ObjectTT) ) {
            return false;
        }
        return _content.equals( ( (ObjectTT) obj ).getContent() );
    }

    public String toString()
    {
        return( "ObjectTT {content='" + _content + "'}" );
    }

} // ObjectTT

}



