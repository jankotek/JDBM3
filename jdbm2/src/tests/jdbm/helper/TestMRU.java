///**
// * JDBM LICENSE v1.00
// *
// * Redistribution and use of this software and associated documentation
// * ("Software"), with or without modification, are permitted provided
// * that the following conditions are met:
// *
// * 1. Redistributions of source code must retain copyright
// *    statements and notices.  Redistributions must also contain a
// *    copy of this document.
// *
// * 2. Redistributions in binary form must reproduce the
// *    above copyright notice, this list of conditions and the
// *    following disclaimer in the documentation and/or other
// *    materials provided with the distribution.
// *
// * 3. The name "JDBM" must not be used to endorse or promote
// *    products derived from this Software without prior written
// *    permission of Cees de Groot.  For written permission,
// *    please contact cg@cdegroot.com.
// *
// * 4. Products derived from this Software may not be called "JDBM"
// *    nor may "JDBM" appear in their names without prior written
// *    permission of Cees de Groot.
// *
// * 5. Due credit should be given to the JDBM Project
// *    (http://jdbm.sourceforge.net/).
// *
// * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
// * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
// * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
// * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
// * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
// * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
// * OF THE POSSIBILITY OF SUCH DAMAGE.
// *
// * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
// * Contributions are Copyright (C) 2000 by their associated contributors.
// *
// */
//
//package jdbm.helper;
//
//import java.util.Enumeration;
//import java.util.NoSuchElementException;
//import java.util.Vector;
//
//import junit.framework.TestSuite;
//
///**
// * Unit test for {@link MRU}.
// * 
// * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
// * @author <a href="mailto:dranatunga@users.sourceforge.net">Dilum Ranatunga</a>
// * @version $Id: TestMRU.java,v 1.3 2003/11/01 13:27:18 dranatunga Exp $
// */
//public class TestMRU extends TestCachePolicy {
//
//    public TestMRU(String name) {
//        super(name);
//    }
//
//    protected CachePolicy createInstance(int capacity) {
//        return new MRU(capacity);
//    }
//
//    /**
//     * Test constructor
//     */
//    public void testConstructor() {
//
//        try {
//            // should not support 0-size cache
//            MRU m1 = new MRU(0);
//            fail("expected exception");
//        } catch (Exception e) {
//        }
//
//        MRU m5 = new MRU(5);
//    }
//
//    /**
//     * Test eviction
//     */
//    public void testEvict() throws CacheEvictionException {
//        Object o1 = new Object();
//        Object o2 = new Object();
//        Object o3 = new Object();
//        Object o4 = new Object();
//        Object o5 = new Object();
//
//        MRU m1 = new MRU(3);
//
//        m1.put("1", o1);
//        m1.put("2", o2);
//        m1.put("3", o3);
//        m1.put("4", o4);
//
//        assertEquals(null, m1.get("1"));
//        assertEquals(o2, m1.get("2"));
//        assertEquals(o3, m1.get("3"));
//        assertEquals(o4, m1.get("4"));
//    }
//
//    /**
//     * Test key replacement
//     */
//    public void testReplace() throws CacheEvictionException {
//        Object o1 = new Object();
//        Object o2 = new Object();
//        Object o3 = new Object();
//        Object o4 = new Object();
//
//        MRU m1 = new MRU(3);
//
//        m1.put("1", o1);
//        m1.put("2", o2);
//        m1.put("3", o3);
//        m1.put("1", o4);
//
//        assertEquals(o4, m1.get("1"));
//        assertEquals(o2, m1.get("2"));
//        assertEquals(o3, m1.get("3"));
//    }
//
//    /**
//     * Test multiple touch
//     */
//    public void testMultiple() throws CacheEvictionException {
//        Object o1 = new Object();
//        Object o2 = new Object();
//        Object o3 = new Object();
//        Object o4 = new Object();
//
//        MRU m1 = new MRU(3);
//
//        m1.put("1", o1);
//        m1.put("2", o2);
//        m1.put("3", o3);
//        m1.put("3", o3);
//        m1.put("3", o3);
//        m1.put("3", o3);
//
//        assertEquals(o1, m1.get("1"));
//        assertEquals(o2, m1.get("2"));
//        assertEquals(o3, m1.get("3"));
//
//        m1.put("1", o3);  // replace with o3
//        m1.put("4", o4);  // should evict 2
//
//        assertEquals(o4, m1.get("4"));
//        assertEquals(o3, m1.get("3"));
//        assertEquals(o3, m1.get("1"));
//        assertEquals(null, m1.get("2"));
//    }
//
//    public void testEvictionExceptionRecovery() throws CacheEvictionException {
//        final CachePolicy cache = new MRU(1);
//        final Object oldKey = "to-be-evicted";
//        final Object newKey = "insert-attempt";
//
//        { // null test
//            cache.removeAll();
//            cache.put(oldKey, new Object());
//            assertNotNull(cache.get(oldKey));
//            cache.put(newKey, new Object());
//            assertNull(cache.get(oldKey));
//            assertNotNull(cache.get(newKey));
//        }
//
//        { // stability test.
//            cache.removeAll();
//            cache.addListener(new ThrowingListener());
//            cache.put(oldKey, new Object());
//            assertNotNull(cache.get(oldKey));
//            try {
//                cache.put(newKey, new Object());
//                fail("Did not propagate expected exception.");
//            } catch (CacheEvictionException cex) {
//                assertNotNull("old object missing after eviction exception!",
//                              cache.get(oldKey));
//                assertNull("new key -> object mapping added even when eviction exception!",
//                           cache.get(newKey));
//            }
//        }
//    }
//
//    /**
//	 * <p>
//	 * This test models a situation in a cache eviction from the hard reference
//	 * cache has a side effect that causes another object to enter the cache.
//	 * The test verifies that the secondary cache eviction is correctly handled
//	 * as a temporary over capacity condition (no secondary eviction is
//	 * performed) and that the cache returns to capacity when the primary cache
//	 * eviction has been completed. The net result is that both the LRU and the
//	 * penultimate LRU objects are evicted from the cache and that the objects
//	 * entering the cache enter in the MRU and penultimate MRU positions.
//	 * </p>
//	 * 
//	 * @see MRU#put(Object, Object, boolean)
//	 */
//    public void test_nextedCacheEvictionCausesTemporaryOverCapacity()
//    	throws CacheEvictionException
//    {
//    	
//        final int CAPACITY = 4;
//        MRU cache = new MRU( CAPACITY );
//
//        Object[] oid = new Object[] {
//          new Long(10), new Long(11), new Long(12),
//				new Long(13), new Long(14), new Long(15)
//        };
//        
//        Object[] obj = new Object[] {
//                new String("o10"),
//                new String("o11"),
//                new String("o12"),
//                new String("o13"),
//                new String("o14"),
//                new String("o15")
//        };
//
//        /*
//		 * Set our cache eviction listener to add the described entry into the
//		 * cache when it receives a cache eviction event.
//		 */
//        MyCacheListenerAddsEntry l = new MyCacheListenerAddsEntry(cache,
//				oid[5], obj[5]);
//        cache.addListener( l );
//        l.denyEvents();
//        
//        /*
//		 * Fill the cache to capacity and verify its state.
//		 */
//        cache.put(oid[0], obj[0]);
//        cache.put(oid[1], obj[1]);
//        cache.put(oid[2], obj[2]);
//        cache.put(oid[3], obj[3]);
//        showCache(cache);
//        assertSameEntryOrdering("entry ordering",new CacheEntry[]{
//                new CacheEntry(oid[0],obj[0]), // LRU
//                new CacheEntry(oid[1],obj[1]),
//                new CacheEntry(oid[2],obj[2]),
//                new CacheEntry(oid[3],obj[3]), // MRU
//        	},
//        	cache.entries() );
//                
//        /*
//		 * Force a cache eviction event by adding another entry to the cache.
//		 * 
//		 * The expected sequence is:
//		 *   
//		 *   put( 4 )
//		 *     objectEvicted( 0 ) - purge LRU from cache.
//		 *        put( 5 ) - does not cause eviction in nested put(), so cache is over capacity.
//		 *     objectEvicted( 1 ) - purge LRU from cache (now at one under capacity)
//		 * 
//		 * Since objects do not enter the cache until after the objectEvicted event
//		 * has been served, this means that 4 is in the MRU position and 5 is in the
//		 * penultimate MRU position since the put() for 4 completes _after_ the nested
//		 * put() for 5.  This is reflected in the cache order test below.
//		 */
//        l.addExpectedEvent(oid[0], obj[0], true, null);
//        l.addExpectedEvent(oid[1], obj[1], true, null);
//        cache.put(oid[4], obj[4]);
//        l.denyEvents();
//        
//        /*
//		 * Verify the state of the cache afterwards. The cache should be back at
//		 * capacity after a brief over capacity while handling the nested put()
//		 * of an object not in the cache.
//		 */
//        showCache( cache );
//        assertSameEntryOrdering("entry ordering",new CacheEntry[]{
//                new CacheEntry(oid[2],obj[2]), // LRU position.
//                new CacheEntry(oid[3],obj[3]),
//                new CacheEntry(oid[5],obj[5]), 
//                new CacheEntry(oid[4],obj[4]), // MRU position
//        	},
//        	cache.entries() );
//        
//    }
//    
//    /**
//	 * This tests concurrent modification of the LRU ordering during traveral.
//	 * 
//	 * @see LRUIterator
//	 */
//    public void test_concurrentModificationDuringTraveral() throws CacheEvictionException {
//
//        final int CAPACITY = 4;
//        MRU cache = new MRU( CAPACITY );
//
//        Long[] oid = new Long[] {
//          new Long(0), new Long(1), new Long(2), new Long(3), new Long(4)      
//        };
//        
//        Object[] obj = new Object[] {
//                new String("o0"),
//                new String("o1"),
//                new String("o2"),
//                new String("o3"),
//                new String("o4")
//        };
//
//        /*
//		 * Set our cache eviction listener to add the described entry into the
//		 * cache when it receives a cache eviction event.
//		 */
//        MyCacheListener l = new MyCacheListener();
//        cache.addListener( l );
//        l.denyEvents();
//        
//        /*
//		 * Fill the cache to capacity and verify its state.
//		 */
//        cache.removeAll();
//        cache.put(oid[0], obj[0]);
//        cache.put(oid[1], obj[1]);
//        cache.put(oid[2], obj[2]);
//        cache.put(oid[3], obj[3]);
//        assertSameEntryOrdering("entry ordering",new CacheEntry[]{
//                new CacheEntry(oid[0],obj[0]),
//                new CacheEntry(oid[1],obj[1]),
//                new CacheEntry(oid[2],obj[2]),
//                new CacheEntry(oid[3],obj[3]),
//        	},
//        	cache.entries() );
//
//        /*
//         * Verify state under one at a time iteration.
//         */
//        Enumeration en = cache.entries();
//        assertSameEntry("LRU[0]", new CacheEntry(oid[0],obj[0]), (CacheEntry)en.nextElement() );
//        assertSameEntry("LRU[1]", new CacheEntry(oid[1],obj[1]), (CacheEntry)en.nextElement() );
//        assertSameEntry("LRU[2]", new CacheEntry(oid[2],obj[2]), (CacheEntry)en.nextElement() );
//        assertSameEntry("LRU[3]", new CacheEntry(oid[3],obj[3]), (CacheEntry)en.nextElement() );
//        assertFalse( en.hasMoreElements() );
//        try {
//        	en.nextElement();
//        	fail("Expecting: "+NoSuchElementException.class);
//        }
//        catch(NoSuchElementException ex) {
//        	System.err.println("Ignoring expected exception: "+ex);
//        }
//        
//        /*
//		 * Verify state under one at a time iteration with concurrent
//		 * modification.
//		 * 
//		 * This removes the first entry before we visit it.
//		 */
//        en = cache.entries();
//        cache.remove(oid[0]);
////        assertSameEntry("LRU[0]", new CacheEntry(oid[0],obj[0]), (CacheEntry)en.nextElement() );
//        assertSameEntry("LRU[1]", new CacheEntry(oid[1],obj[1]), (CacheEntry)en.nextElement() );
//        assertSameEntry("LRU[2]", new CacheEntry(oid[2],obj[2]), (CacheEntry)en.nextElement() );
//        assertSameEntry("LRU[3]", new CacheEntry(oid[3],obj[3]), (CacheEntry)en.nextElement() );
//        assertTrue( !en.hasMoreElements() );
//        try {
//        	en.nextElement();
//        	fail("Expecting: "+NoSuchElementException.class);
//        }
//        catch(NoSuchElementException ex) {
//        	System.err.println("Ignoring expected exception: "+ex);
//        }
//
//        /*
//		 * Fill the cache to capacity and verify its state.
//		 */
//        cache.removeAll();
//        cache.put(oid[0], obj[0]);
//        cache.put(oid[1], obj[1]);
//        cache.put(oid[2], obj[2]);
//        cache.put(oid[3], obj[3]);
//        assertSameEntryOrdering("entry ordering",new CacheEntry[]{
//                new CacheEntry(oid[0],obj[0]),
//                new CacheEntry(oid[1],obj[1]),
//                new CacheEntry(oid[2],obj[2]),
//                new CacheEntry(oid[3],obj[3]),
//        	},
//        	cache.entries() );
//
//        /*
//		 * Verify state under one at a time iteration with concurrent
//		 * modification.
//		 * 
//		 * This removes the 2nd entry before we would visit it.
//		 */
//        en = cache.entries();
//        assertSameEntry("LRU[0]", new CacheEntry(oid[0],obj[0]), (CacheEntry)en.nextElement() );
//        cache.remove(oid[1]);
////        assertSameEntry("LRU[1]", new CacheEntry(oid[1],obj[1]), (CacheEntry)en.nextElement() );
//        assertSameEntry("LRU[2]", new CacheEntry(oid[2],obj[2]), (CacheEntry)en.nextElement() );
//        assertSameEntry("LRU[3]", new CacheEntry(oid[3],obj[3]), (CacheEntry)en.nextElement() );
//        assertFalse( en.hasMoreElements() );
//        try {
//        	en.nextElement();
//        	fail("Expecting: "+NoSuchElementException.class);
//        }
//        catch(NoSuchElementException ex) {
//        	System.err.println("Ignoring expected exception: "+ex);
//        }
//        
//        /*
//		 * Fill the cache to capacity and verify its state.
//		 */
//        cache.removeAll();
//        cache.put(oid[0], obj[0]);
//        cache.put(oid[1], obj[1]);
//        cache.put(oid[2], obj[2]);
//        cache.put(oid[3], obj[3]);
//        assertSameEntryOrdering("entry ordering",new CacheEntry[]{
//                new CacheEntry(oid[0],obj[0]),
//                new CacheEntry(oid[1],obj[1]),
//                new CacheEntry(oid[2],obj[2]),
//                new CacheEntry(oid[3],obj[3]),
//        	},
//        	cache.entries() );
//
//        /*
//		 * Verify state under one at a time iteration with concurrent
//		 * modification.
//		 * 
//		 * This removes the last entry before we would visit it.
//		 */
//        en = cache.entries();
//        assertSameEntry("LRU[0]", new CacheEntry(oid[0],obj[0]), (CacheEntry)en.nextElement() );
//        assertSameEntry("LRU[1]", new CacheEntry(oid[1],obj[1]), (CacheEntry)en.nextElement() );
//        assertSameEntry("LRU[2]", new CacheEntry(oid[2],obj[2]), (CacheEntry)en.nextElement() );
//        cache.remove(oid[3]);
////        assertSameEntry("LRU[3]", new CacheEntry(oid[3],obj[3]), (CacheEntry)en.nextElement() );
//        assertFalse( en.hasMoreElements() );
//        try {
//        	en.nextElement();
//        	fail("Expecting: "+NoSuchElementException.class);
//        }
//        catch(NoSuchElementException ex) {
//        	System.err.println("Ignoring expected exception: "+ex);
//        }
//                
//    }
//
//    /**
//     * Test helper verifies the expected state of a cache entry.
//     * 
//     * @param msg
//     * @param expected
//     * @param actual
//     */
//    static void assertSameEntry(String msg, CacheEntry expected, CacheEntry actual ) {
//    	assertEquals(msg+": oid", expected.getKey(), actual.getKey() );
//    	assertEquals(msg+": value", expected.getValue(), actual.getValue() );
//    	assertEquals(msg+": dirty", expected.isDirty(), actual.isDirty() );
//    	assertEquals(msg+": ser", expected.getSerializer(), actual.getSerializer() );
//    }
//    
//    /**
//     * Verify that the cache iterator visit {@link CacheEntry}instances that
//     * are consistent with the expected entries in both order and data. The data
//     * consistency requirements are: (a) same oid/key (compared by value); same
//     * value object associated with that key (compared by reference); and same
//     * dirty flag state.
//     * 
//     * @param msg
//     *            Message.
//     * @param expected
//     *            Array of expected cache entry objects in expected order.
//     * @param actual
//     *            Iterator visiting {@link CacheEntry}objects.
//     */
//
//    static void assertSameEntryOrdering(String msg,CacheEntry expected[], Enumeration actual ) {
//
//	int i = 0;
//
//	while( actual.hasMoreElements() ) {
//
//	    if( i >= expected.length ) {
//
//		fail( msg+": The enumeration is willing to visit more than "+
//		      expected.length+
//		      " objects."
//		      );
//
//	    }
//
//	    CacheEntry expectedEntry = expected[ i ];
//	    CacheEntry actualEntry = (CacheEntry) actual.nextElement();
//	    
//	    assertEquals
//	        ( msg+": key differs at index="+i,
//	                expectedEntry.getKey(),
//	                actualEntry.getKey()
//	                );
//
//	    assertTrue
//	        ( msg+": value references differ at index="+i+", expected="+expected+", actual="+actual,
//	                expectedEntry.getValue() == actualEntry.getValue()
//	                );	    
//
//	    assertEquals
//	        ( msg+": dirty flag differs at index="+i,
//	                expectedEntry.isDirty(),
//	                actualEntry.isDirty()
//	                );
//
//	    i++;
//
//	}
//
//	if( i < expected.length ) {
//
//	    fail( msg+": The iterator SHOULD have visited "+expected.length+
//		  " objects, but only visited "+i+
//		  " objects."
//		  );
//
//	}
//
//    }
//
//    /**
//	 * Dumps the contents of the cache on {@link System#err} using
//	 * {@link CachePolicy#entryIterator()}.
//	 * 
//	 */
//    static void showCache(CachePolicy cache) {
//    	System.err.println("\nshowCache: "+cache.getClass());
//    	Enumeration en = cache.entries();
//    	int i = 0;
//    	while( en.hasMoreElements() ) {
//    		CacheEntry entry = (CacheEntry) en.nextElement();
//    		System.err.println("[" + i + "]\tkey=" + entry.getKey()
//					+ ", value=" + entry.getValue() + ", dirty="
//					+ entry.isDirty() + ", ser=" + entry.getSerializer());
//    		i++;
//    	}
//    }
//    
//    /**
//     * You set whether or not a cache event is expected and what the expected
//     * data will be for that event. If an event occurs when none is expected
//     * then an exception is thrown. If an event occurs with unexpected data then
//     * an exception is thrown. Otherwise the listener silently accepts the
//     * event.
//     */
//    public static class MyCacheListener implements CachePolicyListener
//    {
//        private boolean expectingEvent = false;
//        private boolean haveEvent = false;
//        private static class Event {
//            private Object expectedOid = null;
//            private Object expectedObj = null;
//            private boolean expectedDirty = false;
//            private Serializer expectedSerializer = null;
//        }
//        private Vector events = new Vector();
//        
//        /**
//         * Verify that event data is consistent with our expectations.
//         * 
//         * @exception IllegalStateException
//         *                If we already have an event.
//         * @exception AssertionFailedException
//         *                If we are not expecting an event.
//         * @exception AssertionFailedException
//         *                If the object identifier or object in the event are
//         *                incorrect. The objects are compared by reference, not
//         *                by equals().
//         */
//        public void cacheObjectEvicted(Object key, Object obj, boolean dirty,
//				Serializer ser) throws CacheEvictionException {
//        	if(!expectingEvent) {
//        		throw new IllegalStateException("Not expecting event");
//        	}
//            if( haveEvent ) {
//                throw new IllegalStateException("Already have an event");
//            }
//            haveEvent = true;
//            if( events.size() == 0 ) {
//            	throw new IllegalStateException("No expected events");
//            }
//            Event e = (Event) events.remove(0); // pop off next event.
//            assertEquals("oid",e.expectedOid,key);
//            assertTrue("obj",e.expectedObj == obj); // compare by reference not equals().
//            assertEquals("dirty",e.expectedDirty,dirty);
//            assertEquals("serializer", e.expectedSerializer, ser );
//        }
//
//        /**
//		 * Sets the listener to expect an event with the given object identifier
//		 * and object.
//		 * 
//		 * @param oid
//		 *            The expected object identifier.
//		 * @param obj
//		 *            The expected object (comparison by reference).
//		 * @param dirty
//		 *            Iff the expected object will be marked as dirty.
//		 * 
//		 * @see #clearLastEvent()
//		 * @see #denyEvents()
//		 */
//        public void setExpectedEvent(Object oid,Object obj,boolean dirty, Serializer ser) {
//        	clearExpectedEvents();
//        	addExpectedEvent(oid,obj,dirty,ser);
//        }
//
//        public void clearExpectedEvents() {
//        	events.clear();
//        	denyEvents();
//        }
//        
//        public void addExpectedEvent( Object oid, Object obj, boolean dirty, Serializer ser) {
//        	Event e = new Event();
//            e.expectedOid = oid;
//            e.expectedObj = obj;
//            e.expectedDirty = dirty;
//            e.expectedSerializer = ser;
//            events.add(e);
//            allowEvents();
//        }
//        
//        /**
//         * Causes an {@link IllegalStateException} to be thrown from the
//         * listener if an event is received.
//         * 
//         * @see #setExpectedEvent(long, Object)
//         */
//        public void denyEvents()
//        {
//            expectingEvent = false;
//        }
//
//        /**
//         * Allows more events.  If an event had already been received, then it
//         * is cleared now.
//         */
//        public void allowEvents() {
//        	expectingEvent = true;
//        	if( haveEvent ) {
//        		clearLastEvent();
//        	}
//        }
//        
//        /**
//         * Clear the last event so that a new event may be accepted. An
//         * exception is thrown if no event has been received so that this method
//         * may be used to test for the absence of an expected event.
//         * 
//         * @exception IllegalStateException
//         *                if no event has been received.
//         */
//        public void clearLastEvent() {
//            if( ! haveEvent ) {
//                throw new IllegalStateException("no event");
//            }
//            haveEvent = false;
//        }
//    }
//
//    /**
//	 * Implementation adds an entry to the cache when it receives a cache
//	 * eviction notice. The cache entry to be added is described by the
//	 * parameters to the constructor. This is a one-time behavior. The listener
//	 * will log subsequent events but otherwise take no action.
//	 * 
//	 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//	 * @version $Id: TestMRU.java,v 1.7 2006/05/29 19:44:32 thompsonbry Exp $
//	 */
//    public static class MyCacheListenerAddsEntry extends MyCacheListener
//    {
//    	private final CachePolicy _cache;
//    	private final Object _oid;
//    	private final Object _obj;
//    	private final boolean _dirty;
//    	private final Serializer _ser;
//    	private int _nevents = 0;
//    	
//    	public MyCacheListenerAddsEntry( CachePolicy cache, Object oid, Object obj, boolean dirty, Serializer ser ) {
//    		super();
//    		this._cache = cache;
//    		this._oid = oid;
//    		this._obj = obj;
//    		this._dirty = dirty;
//    		this._ser = ser;
//    	}
//    	
//		public void cacheObjectEvicted(Object oid, Object obj, boolean dirty,
//				Serializer ser) throws CacheEvictionException {
//			super.cacheObjectEvicted(oid, obj, dirty, ser );
//			_nevents++;
//	    	showCache(_cache);
//			System.err.println("objectEvicted("+oid+"), _nevents="+_nevents);
//			if( _nevents == 1 ) {
//				denyEvents(); // not expecting an eviction.
//				_cache.put(_oid, _obj, _dirty, _ser);
//				allowEvents(); // allow next expected eviction.
//			}
//		}
//		
//    }
//    
//
//    /**
//     * Runs all tests in this class
//     */
//    public static void main(String[] args) {
//        junit.textui.TestRunner.run(new TestSuite(TestMRU.class));
//    }
//}
