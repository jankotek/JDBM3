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


package jdbm.btree;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.AbstractMap.SimpleEntry;

import jdbm.RecordListener;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.RecordManagerOptions;
import jdbm.helper.ByteArrayComparator;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;
import jdbm.recman.TestCaseWithTestFile;
import junit.framework.AssertionFailedError;
import junit.framework.TestResult;
import junit.framework.TestSuite;

/**
 *  This class contains all Unit tests for {@link BTree}.
 *
 *  @author <a href="mailto:boisvert@exoffice.com">Alex Boisvert</a>
 *  @version $Id: TestBTree.java,v 1.8 2003/09/21 15:49:02 boisvert Exp $
 */
public class TestBTree
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
        RecordManager  recman;
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
            System.out.println("TestBTree.testBasics");
        }

        recman = newRecordManager();
        tree = BTree.createInstance( recman, new ByteArrayComparator() );

        tree.insert( test1, value1, false );
        tree.insert( test2, value2, false );

        byte[] result;
        result = (byte[]) tree.find( test0 );
        if ( result != null ) {
            throw new Error( "Test0 shouldn't be found" );
        }

        result = (byte[]) tree.find( test1 );
        if ( result == null || ByteArrayComparator.compareByteArray( result, value1 ) != 0 ) {
            throw new Error( "Invalid value for test1: " + result );
        }

        result = (byte[]) tree.find( test2 );
        if ( result == null || ByteArrayComparator.compareByteArray( result, value2 ) != 0 ) {
            throw new Error( "Invalid value for test2: " + result );
        }

        result = (byte[]) tree.find( test3 );
        if ( result != null ) {
            throw new Error( "Test3 shouldn't be found" );
        }

        recman.close();
    }

    /**
     *  Basic tests, just use the simple test possibilities of junit (cdaller)
     */
    public void testBasics2() throws IOException {
        RecordManager  recman;
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
            System.out.println("TestBTree.testBasics2");

        recman = newRecordManager();
        tree = BTree.createInstance( recman, new ByteArrayComparator() );

        tree.insert( test1, value1, false);
        tree.insert( test2, value2, false);

        assertEquals( null, tree.find( test0 ) );
        assertEquals( 0, ByteArrayComparator.compareByteArray( value1, (byte[]) tree.find( test1 ) ) );
        assertEquals( 0, ByteArrayComparator.compareByteArray( value2, (byte[]) tree.find( test2 ) ) );
        assertEquals( null, (byte[]) tree.find( test3 ) );

        recman.close();
     }


    /**
     *  Test what happens after the recmanager has been closed but the
     *  btree is accessed. WHAT SHOULD HAPPEN???????????
     * (cdaller)
     */
    public void testClose()
        throws IOException
    {
        RecordManager  recman;
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
            System.out.println("TestBTree.testClose");

        recman = newRecordManager();
        tree = BTree.createInstance( recman, new ByteArrayComparator() );

        tree.insert( test1, value1, false );
        tree.insert( test2, value2, false );

        assertEquals( null, tree.find( test0 ) );
        assertEquals( 0, ByteArrayComparator.compareByteArray( value1, (byte[]) tree.find( test1 ) ) );
        assertEquals( 0, ByteArrayComparator.compareByteArray( value2, (byte[]) tree.find( test2 ) ) );
        assertEquals( null, (byte[]) tree.find( test3 ) );

        recman.close();

        try {
            tree.browse();
            fail("Should throw an IllegalStateException on access on not opened btree");
        } catch( IllegalStateException except ) {
            // ignore
        }

        try {
            tree.find( test0 );
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
        RecordManager  recman;
        BTree          tree;

        if ( DEBUG )
            System.out.println("TestBTree.testInsert");

        recman = newRecordManager();
        tree = BTree.createInstance( recman);

        // insert differnt objects and retrieve them
        tree.insert("test1", "value1",false);
        tree.insert("test2","value2",false);
        tree.insert("one", new Integer(1),false);
        tree.insert("two",new Long(2),false);
        tree.insert("myownobject",new ObjectTT(new Integer(234)),false);

        assertEquals("value2",(String)tree.find("test2"));
        assertEquals("value1",(String)tree.find("test1"));
        assertEquals(new Integer(1),(Integer)tree.find("one"));
        assertEquals(new Long(2),(Long)tree.find("two"));

        // what happens here? must not be replaced, does it return anything?
        // probably yes!
        assertEquals("value1",tree.insert("test1","value11",false));
        assertEquals("value1",tree.find("test1")); // still the old value?
        assertEquals("value1",tree.insert("test1","value11",true));
        assertEquals("value11",tree.find("test1")); // now the new value!

        ObjectTT expected_obj = new ObjectTT(new Integer(234));
        ObjectTT btree_obj = (ObjectTT)tree.find("myownobject");
        assertEquals(expected_obj, btree_obj);

        recman.close();
    }


    /**
     *  Test to remove  objects from the btree. (cdaller)
     */
    public void testRemove()
        throws IOException
    {
        RecordManager  recman;
        BTree          tree;

        if ( DEBUG ) {
            System.out.println( "TestBTree.testRemove" );
        }

        recman = newRecordManager();
        tree = BTree.createInstance( recman);

        tree.insert("test1", "value1",false);
        tree.insert("test2","value2",false);
        assertEquals("value1",(String)tree.find("test1"));
        assertEquals("value2",(String)tree.find("test2"));
        tree.remove("test1");
        assertEquals(null,(String)tree.find("test1"));
        assertEquals("value2",(String)tree.find("test2"));
        tree.remove("test2");
        assertEquals(null,(String)tree.find("test2"));

        int iterations = 1000;

        for ( int count = 0; count < iterations; count++ ) {
            tree.insert( "num"+count, new Integer( count ), false );
        }

        assertEquals( iterations, tree.size() );

        for ( int count = 0; count < iterations; count++ ) {
            assertEquals( new Integer( count ), tree.find( "num" + count ) );
        }

        for ( int count = 0; count < iterations; count++ ) {
           tree.remove( "num" + count );
        }

        assertEquals( 0, tree.size() );

        recman.close();
    }

    /**
     *  Test to find differents objects in the btree. (cdaller)
     */
    public void testFind()
        throws IOException
    {
        RecordManager  recman;
        BTree          tree;

        if ( DEBUG )
            System.out.println("TestBTree.testFind");

        recman = newRecordManager();
        tree = BTree.createInstance( recman);

        tree.insert("test1", "value1",false);
        tree.insert("test2","value2",false);

        Object value = tree.find("test1");
        assertTrue(value instanceof String);
        assertEquals("value1",value);

        tree.insert("","Empty String as key",false);
        assertEquals("Empty String as key",(String)tree.find(""));

        assertEquals(null,(String)tree.find("someoneelse"));

        recman.close();
    }
    /**
     *  Test deletion of btree from record manager. (kday)
     *  
     *  After deletion, the BTree and all of it's BPage children (and their children)
     *  should be removed from the recordmanager.
     */
    public void testDelete()
        throws IOException
    {
        RecordManager  recman;
        BTree<String, Serializable>          tree;
        if ( DEBUG )
            System.out.println("TestBTree.testFind");
        Properties props = new Properties();
        recman = RecordManagerFactory.createRecordManager( newTestFile(), props );
        tree = BTree.createInstance( recman);

        // put enough data into the tree so we definitely have multiple pages
        for (int count = 1; count <= 1000; count++){
            tree.insert("num" + count,new Integer(count),false);
            if (count % 100 == 0)
                recman.commit();
        }
        List<Long> out = new ArrayList<Long>();
        tree.dumpChildPageRecIDs(out);        
        assertTrue(out.size() > 0);
    }


    /**
     *  Test to insert, retrieve and remove a large amount of data. (cdaller)
     */
    public void testLargeDataAmount()
        throws IOException
    {

        RecordManager  recman;
        BTree          tree;

        if ( DEBUG )
            System.out.println("TestBTree.testLargeDataAmount");

        recman = newRecordManager();
        // recman = new jdbm.recman.BaseRecordManager( "test" );
        
        tree = BTree.createInstance( recman);
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

           // find data
         for(int count = 0; count < iterations; count++)
         {
           assertEquals(new Integer(count), tree.find("num"+count));
         }

             // delete data
         for(int count = 0; count < iterations; count++)
         {
           assertEquals(new Integer(count),tree.remove("num"+count));
         }

         assertEquals(0,tree.size());

         recman.close();
   }
    
    public void testRecordListener() throws IOException{
        RecordManager recman = newRecordManager();
        BTree<Integer,String> tree = BTree.createInstance( recman);
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
     * Tests the corner case of deleting all nodes from the tree.  In this case, all BPages
     * associated with the tree should be removed from the recman.
     * 
     * We are also going to test to make sure the recman file doesn't grow (leak) if we repeat the
     * process a number of times.
     * @throws Exception
     */
    public void testDeleteAllNodes() throws Exception {
        RecordManager  recman;
        BTree<String, Serializable>          tree;
        
        // we are going to run this test without object cache enabled.  If it is turned on,
        // we will have problems with using a different deserializer for BPages than the standard
        // serializer.
        Properties props = new Properties();
        props.setProperty(RecordManagerOptions.CACHE_TYPE, "none");

        String recordManagerBasename = newTestFile();
        String recordManagerDBname = recordManagerBasename+".dbr.0";
        
        long previousRecmanSize = 0;
        for (int i = 0; i < 5; i++){
            recman = RecordManagerFactory.createRecordManager( recordManagerBasename, props );
      
            try{
                tree = BTree.createInstance( recman);
                String[] keys = new String[1000];
                for (int count = 0; count < 1000; count++){
                    keys[count] = "num" + count;
                }      
          
                // put enough data into the tree so we definitely have multiple pages
                for (int count = 0; count < 1000; count++){
                    tree.insert(keys[count],new Integer(count),false);
                    if (count % 100 == 0)
                        recman.commit();
                }
                recman.commit();
                
                long currentRecmanSize = new File(recordManagerDBname).length();
                assertTrue("file size too small "+ currentRecmanSize, currentRecmanSize > 0);

                
                // now remove it all
                
                for (int count = 0; count < 1000; count++){
                    tree.remove(keys[count]);
                    if (count % 100 == 0)
                        recman.commit();
                }
                recman.commit();
                
                BPage root = tree.getRoot();
                assertNull(root);
                
            } finally {
                recman.close();
                long currentRecmanSize = new File(recordManagerDBname).length();
                assertTrue("file size too small "+ currentRecmanSize, currentRecmanSize > 0);
                if (previousRecmanSize != 0){
                    assertTrue(currentRecmanSize == previousRecmanSize);
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
        RecordManager  recman;
        BTree          tree;

        if ( DEBUG )
            System.out.println("TestBTree.testMultithreadAccess");

        recman = newRecordManager();
        tree = BTree.createInstance( recman);

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
        recman.close();
    }


  
    /**
     *  Helper method to 'simulate' the methods of an entry set of the btree.
     */
    protected static boolean containsKey( Object key, BTree btree )
        throws IOException
    {
        return ( btree.find( key ) != null );
    }


    /**
     *  Helper method to 'simulate' the methods of an entry set of the btree.
     */
    protected static boolean containsValue( Object value, BTree btree )
        throws IOException
  {
    // we must synchronize on the BTree while browsing
    synchronized ( btree ) {
        TupleBrowser browser = btree.browse();
        Tuple tuple = new Tuple();
        while(browser.getNext(tuple)) {
          if(tuple.getValue().equals(value))
            return(true);
        }
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
        Object tree_obj = btree.find(entry.getKey());
        if ( tree_obj == null ) {
            // can't distuingish, if value is null or not found!!!!!!
            return ( entry.getValue() == null );
        }
        return ( tree_obj.equals( entry.getValue() ) );
    }

    /**
     *  Runs all tests in this class
     */
    public static void main( String[] args ) {
        junit.textui.TestRunner.run( new TestSuite( TestBTree.class ) );
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
                assertEquals( entry.getValue(), _btree.find( entry.getKey() ) );
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
}


/**
 * class for testing puroposes only (store as value in btree) not
 * implemented as inner class, as this prevents Serialization if
 * outer class is not Serializable.
 */
class ObjectTT
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

