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
 * $Id
 */

package jdbm.helper;

import java.util.ArrayList;

import junit.framework.TestCase;

/**
 * Abstract class that provides some convenience test cases,
 * classes and methods for any class that tests an implementation
 * of {@link CachePolicy}.
 * <p/>
 * Concrete subclasses must provide {@link #createInstance(int)}. They may
 * also need to override {@link #causeEviction(CachePolicy, int)} if the
 * default strategy will not suffice for the implementation of being tested.
 * 
 * @author <a href="mailto:dranatunga@users.sourceforge.net">Dilum Ranatunga</a>
 * @version $Id: TestCachePolicy.java,v 1.2 2006/05/03 16:01:18 thompsonbry Exp $
 * 
 * FIXME Add tests for the dirty and serializer metadata fields and the new entries()
 * method.  Make sure that put() causes those metadata fields to be saved on the
 * ICacheEntry, that the metadata are correctly reported by entries() and by cache
 * eviction notices.
 */
public abstract class _TestCachePolicy extends TestCase {

    protected _TestCachePolicy(String string) {
        super(string);
    }

    /**
     * Factory that test methods use to create test instances. The instance
     * should be capable of storing the specified number of <q>large objects</q>
     * without an eviction. Large objects are created by {@link #createLargeObject()}.
     * 
     * @param capacity the number of large objects the instance should be
     *     capable of containing.
     * @return a non-null cache instance.
     */
    protected abstract CachePolicy createInstance(int capacity);

    /**
     * Causes at least one eviction from the specified cache.
     * <p>
     * Note: take care when overriding this with implementations that depend
     * on adding a temporary listener to the cache. Such a scheme can cause
     * an infinite loop when <em>testing adding and removing listeners</em>.
     *
     * @param cache a cache object. It is generally safe to assume that this
     *     was created using {@link #createInstance(int)}.
     * @param capacity the capacity used when cache was created.
     * @throws CacheEvictionException
     */
    protected void causeEviction(final CachePolicy cache, final int capacity) throws CacheEvictionException {
        for (int i = 0; i < capacity; ++i) {
            cache.put(-i, new Object()); //negative values are very likely not used
        }
    }

    /**
     * Tests {@link CachePolicy#addListener(CachePolicyListener)},
     * {@link CachePolicy#removeListener(CachePolicyListener)}.
     */
    public void testAddRemoveListeners() {
        try {
            final CountingListener listener1 = new CountingListener("Listener1");
            final CountingListener listener2_1 = new CountingListener("Listener2");
            final CountingListener listener2_2 = new CountingListener("Listener2");
            final int capacity = 2;
            final CachePolicy cache = createInstance(capacity);

            { // quick check of assumptions
                assertTrue("Listeners should be equal.",
                           listener2_1.equals(listener2_2));
                assertTrue("Equal listeners' hashcodes should be equal.",
                           listener2_1.hashCode() == listener2_2.hashCode());
            }

            { // bad input test
                try {
                    cache.addListener(null);
                    fail("cache.addListener(null) should throw IllegalArgumentRxception.");
                } catch (IllegalArgumentException e) { }
            }

            { // null test
                causeEviction(cache, capacity);
                assertEquals(0, listener1.count());
                assertEquals(0, listener2_1.count());
                assertEquals(0, listener2_2.count());
            }

            { // show that add affects cache, listener
                cache.addListener(listener1);
                causeEviction(cache, capacity);
                assertTrue("listener not getting added, "
                           + "not getting eviction event "
                           + " or causeEviction not working)",
                           listener1.count() > 0);
            }

            { // show that remove affects cache, listener
                listener1.reset();
                cache.removeListener(listener1);
                causeEviction(cache, capacity);
                assertTrue("listener not getting removed",
                           listener1.count() == 0);
            }

            { // show that multiple listeners are used
                listener1.reset();
                listener2_1.reset();
                listener2_2.reset();
                cache.addListener(listener1);
                cache.addListener(listener2_1);
                cache.addListener(listener2_2);
                causeEviction(cache, capacity);
                assertTrue(listener1.count() > 0);
                // note XOR: only one of the listeners should have received the event.
                assertTrue((listener2_1.count() > 0) ^ (listener2_2.count() > 0));
            }

            {
                // show that multiple adds of equal listeners is undone with single remove
                cache.removeListener(listener2_1);
                cache.removeListener(listener2_2);
                listener2_1.reset();
                listener2_2.reset();
                causeEviction(cache, capacity);
                assertTrue((listener2_1.count() == 0) && (listener2_2.count() == 0));

                cache.addListener(listener2_1);
                cache.addListener(listener2_2);
                causeEviction(cache, capacity);
                assertTrue((listener2_1.count() > 0) ^ (listener2_2.count() > 0));

                listener2_1.reset();
                listener2_2.reset();
                cache.removeListener(listener2_1); // note: only one is removed.
                causeEviction(cache, capacity);
                assertTrue((listener2_1.count() == 0) && (listener2_2.count() == 0));
            }
        } catch (CacheEvictionException cex) {
            fail("Cache is throwing eviction exceptions even though none of the listeners are.");
        }
    }

    /**
     * Ensures that the {@link CachePolicy} implementation propagates
     * {@link CacheEvictionException}s back to the caller.
     */
    public void testEvictionExceptionPropagation() throws CacheEvictionException {
        final CachePolicyListener quietListener = new CountingListener("quiet");
        final CachePolicyListener throwingListener = new ThrowingListener();
        final int capacity = 1;
        final CachePolicy cache = createInstance(capacity);
        { // null test.
            cache.addListener(quietListener);
            cache.removeAll();
            try {
                causeEviction(cache, capacity);
            } catch (CacheEvictionException cex) {
                fail("Threw eviction exception when it wasn't supposed to: " + cex);
            }
            cache.removeListener(quietListener);
        }

        { // propagation test
            cache.addListener(throwingListener);
            try {
                causeEviction(cache, capacity);
                fail("Did not propagate expected exception.");
            } catch (CacheEvictionException cex) { }
            cache.removeListener(throwingListener);
        }
    }

    protected void causeGarbageCollection() {
        try {
            ArrayList l = new ArrayList();
            for (int i = 0; i < 500; ++i) {
                l.add(createLargeObject());
            }
        } catch (OutOfMemoryError oome) {
        }
        for (int i = 0; i < 10; ++i) {
            System.gc();
        }
    }

    protected Object createLargeObject() {
        int[] a = new int[1024 * 1024]; // 1M of ints.
        // Fill the array. This is done to prevent any sneaky VMs from
        // saving  space by lazily allocating the full array.
        for (int i = a.length; --i >= 0;) {
            a[i] = i;
        }
        return a;
    }

    /**
     * Listener used to test whether the event method is being invoked.
     * Typical usage idiom is of the form:
     * <pre>
     * CachePolicy cache = ...;
     * CountingListener listener = new CountingListener("mylistener");
     * ...
     * listener.reset();
     * cache.addListener(listener);
     * // do stuff with cache
     * assertTrue(listener.count() > 0);
     * </pre>
     */
    protected static final class CountingListener implements CachePolicyListener {
        private String _name;
        private int _count = 0;

        /**
         * Creates a counting listener with the name specified.
         * @param name the (non-null) name of the listener.
         */
        CountingListener(String name) {
            _name = new String(name); // this automatically throws NPE if name is null.
        }

        /**
         * Implimentation of callback method that increments count.
         */
        public void cacheObjectEvicted(Object obj) throws CacheEvictionException {
            _count++;
        }

        /**
         * Reset's this listener's count to zero.
         */
        void reset() {
            _count = 0;
        }

        /**
         * Gets this listener's current count: the number of times the
         * callback method's been invoked since creation/reset.
         * @return
         */
        int count() {
            return _count;
        }

        /**
         * Equality defined as (same type) AND (names equal).
         */
        public boolean equals(Object obj) {
            if (!(obj instanceof CountingListener)) {
                return false;
            }
            return _name.equals(((CountingListener) obj)._name);
        }

        /**
         * Defined as hashcode of name; consistent with {@link #equals(Object)}.
         */
        public int hashCode() {
            return _name.hashCode();
        }
    }

    /**
     * Listener used to cause a {@link CacheEvictionException}.
     * Typical usage idiom is of the form:
     * <pre>
     * CachePolicy cache = ...;
     * ThrowingListener listener = new ThrowingListener();
     * ...
     * // cause cache evictions without exceptions.
     * cache.addListener(listener);
     * try {
     *   // cause a cache eviction.
     *   fail("exception expected");
     * } catch (CacheEvictionException e) { }
     * </pre>
     */
    protected static final class ThrowingListener implements CachePolicyListener {

        protected static final String MESSAGE = "Intentionally thrown for testing purposes.";

        /**
         * Always throws a {@link CacheEvictionException} wrapping a
         * runtime exception with the message {@link #MESSAGE}.
         */
        public void cacheObjectEvicted(Object obj) throws CacheEvictionException {
            throw new CacheEvictionException(new RuntimeException(MESSAGE));
        }
    }
}
