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
 * $Id: TestMRUNativeLong.java,v 1.2 2006/05/03 16:01:18 thompsonbry Exp $
 */

package jdbm.helper;


/**
 * Unit test for {@link MRUNativeLong}.
 * 
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @author <a href="mailto:dranatunga@users.sourceforge.net">Dilum Ranatunga</a>
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestMRUNativeLong extends _TestCachePolicy {

    public TestMRUNativeLong(String name) {
        super(name);
    }

    protected CachePolicy createInstance(int capacity) {
        return new MRU(capacity);
    }

    /**
     * Test constructor
     */
    public void testConstructor() {

        try {
            // should not support 0-size cache
            new MRU(0);
            fail("expected exception");
        } catch (Exception e) {
        }

        new MRU(5);
    }

    /**
     * Test eviction
     */
    public void testEvict() throws CacheEvictionException {
        Object o1 = new Object();
        Object o2 = new Object();
        Object o3 = new Object();
        Object o4 = new Object();

        CachePolicy m1 = new MRU(3);

        m1.put(new Long(1), o1);
        m1.put(new Long(2), o2);
        m1.put(new Long(3), o3);
        m1.put(new Long(4), o4);

        assertEquals(null, m1.get(new Long(1)));
        assertEquals(o2, m1.get(new Long(2)));
        assertEquals(o3, m1.get(new Long(3)));
        assertEquals(o4, m1.get(new Long(4)));
    }

    /**
     * Test key replacement
     */
    public void testReplace() throws CacheEvictionException {
        Object o1 = new Object();
        Object o2 = new Object();
        Object o3 = new Object();
        Object o4 = new Object();

        CachePolicy m1 = new MRU(3);

        m1.put(new Long(1), o1);
        m1.put(new Long(2), o2);
        m1.put(new Long(3), o3);
        m1.put(new Long(1), o4);

        assertEquals(o4, m1.get(new Long(1)));
        assertEquals(o2, m1.get(new Long(2)));
        assertEquals(o3, m1.get(new Long(3)));
    }

    /**
     * Test multiple touch
     */
    public void testMultiple() throws CacheEvictionException {
        Object o1 = new Object();
        Object o2 = new Object();
        Object o3 = new Object();
        Object o4 = new Object();

        CachePolicy m1 = new MRU(3);

        m1.put(new Long(1), o1);
        m1.put(new Long(2), o2);
        m1.put(new Long(3), o3);
        m1.put(new Long(3), o3);
        m1.put(new Long(3), o3);
        m1.put(new Long(3), o3);

        assertEquals(o1, m1.get(new Long(1)));
        assertEquals(o2, m1.get(new Long(2)));
        assertEquals(o3, m1.get(new Long(3)));

        m1.put(new Long(1), o3);  // replace with o3
        m1.put(new Long(4), o4);  // should evict 2

        assertEquals(o4, m1.get(new Long(4)));
        assertEquals(o3, m1.get(new Long(3)));
        assertEquals(o3, m1.get(new Long(1)));
        assertEquals(null, m1.get(new Long(2)));
    }

    /**
     * Override the default implementation which does not use Long keys.
     * The default implementation uses <code>new Object()</code> for the
     * key to force an eviction.  This works since all object instances
     * are distinct.  However the {@link MRUNativeLong} class uses Long
     * keys, so we can not rely on the same trick.  This has been written
     * to use negative values for force evications, so be careful not to
     * use keys with negative values in the tests that you write in this
     * suite.
     */
    protected void causeEviction(final CachePolicy cache, final int capacity) throws CacheEvictionException {
        for (int i = 0; i < capacity; ++i) {
            cache.put(new Long(_onedown--), new Object());
        }
    }
    private long _onedown = -1L;

    public void testEvictionExceptionRecovery() throws CacheEvictionException {
        final CachePolicy cache = new MRU(1);
        final long oldKey = 12l; // "to-be-evicted";
        final long newKey = 18l; // "insert-attempt";

        { // null test
            cache.removeAll();
            cache.put(oldKey, new Object());
            assertNotNull(cache.get(oldKey));
            cache.put(newKey, new Object());
            assertNull(cache.get(oldKey));
            assertNotNull(cache.get(newKey));
        }

        { // stability test.
            cache.removeAll();
            cache.addListener(new ThrowingListener());
            cache.put(oldKey, new Object());
            assertNotNull(cache.get(oldKey));
            try {
                cache.put(newKey, new Object());
                fail("Did not propagate expected exception.");
            } catch (CacheEvictionException cex) {
                assertNotNull("old object missing after eviction exception!",
                              cache.get(oldKey));
                assertNull("new key -> object mapping added even when eviction exception!",
                           cache.get(newKey));
            }
        }
    }

 
}
