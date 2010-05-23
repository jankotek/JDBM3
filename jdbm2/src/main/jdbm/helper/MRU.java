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
 * $Id: MRU.java,v 1.8 2005/06/25 23:12:31 doomdark Exp $
 */

package jdbm.helper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 *  MRU - Most Recently Used cache policy.
 *
 *  Methods are *not* synchronized, so no concurrent access is allowed.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: MRU.java,v 1.8 2005/06/25 23:12:31 doomdark Exp $
 */
public class MRU<V> implements CachePolicy<V> {

    /** Cached object hashtable */
	LongKeyChainedHashMap<CacheEntry<V>> _hash;

    /**
     * Maximum number of objects in the cache.
     */
    int _max;

    /**
     * Beginning of linked-list of cache elements.  First entry is element
     * which has been used least recently.
     */
    CacheEntry<V> _first;

    /**
     * End of linked-list of cache elements.  Last entry is element
     * which has been used most recently.
     */
    CacheEntry<V> _last;


    /**
     * Cache eviction listeners
     */
    List<CachePolicyListener> listeners = new ArrayList<CachePolicyListener>();


    /**
     * Construct an MRU with a given maximum number of objects.
     */
    public MRU(int max) {
        if (max <= 0) {
            throw new IllegalArgumentException("MRU cache must contain at least one entry");
        }
        _hash = new LongKeyChainedHashMap<CacheEntry<V>>(max);
        _max = max;
    }


    /**
     * Place an object in the cache.
     */
    public void put(long key, V value) throws CacheEvictionException {
        CacheEntry<V> entry = _hash.get(key);
        if (entry != null) {
            entry.setValue(value);
            touchEntry(entry);
        } else {

            if (_hash.size() == _max) {
                // purge and recycle entry
                entry = purgeEntry();
                entry.setKey(key);
                entry.setValue(value);
            } else {
                entry = new CacheEntry<V>(key, value);
            }
            addEntry(entry);
            _hash.put(entry.getKey(), entry);
        }
    }


    /**
     * Obtain an object in the cache
     */
    public V get(long key) {
        CacheEntry<V> entry = _hash.get(key);
        if (entry != null) {
            touchEntry(entry);
            return entry.getValue();
        } else {
            return null;
        }
    }


    /**
     * Remove an object from the cache
     */
    public void remove(long key) {
        CacheEntry<V> entry = _hash.get(key);
        if (entry != null) {
            removeEntry(entry);
            _hash.remove(entry.getKey());
        }
    }


    /**
     * Remove all objects from the cache
     */
    public void removeAll() {
        //_hash = new LongKeyChainedHashMap<CacheEntry<V>>(_max);
    	_hash.clear();
        _first = null;
        _last = null;
    }


    /**
     * Enumerate elements' values in the cache
     */
    public Iterator<V> elements() {
        return new MRUIterator<V>(_hash.values().iterator());
    }

    /**
     * Add a listener to this cache policy
     *
     * @param listener Listener to add to this policy
     */
    public void addListener(CachePolicyListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("Cannot add null listener.");
        }
        if ( ! listeners.contains(listener)) {
            listeners.add(listener);
        }
    }

    /**
     * Remove a listener from this cache policy
     *
     * @param listener Listener to remove from this policy
     */
    public void removeListener(CachePolicyListener listener) {
        listeners.remove(listener);
    }

    /**
     * Add a CacheEntry.  Entry goes at the end of the list.
     */
    protected void addEntry(CacheEntry<V> entry) {
        if (_first == null) {
            _first = entry;
            _last = entry;
        } else {
            _last.setNext(entry);
            entry.setPrevious(_last);
            _last = entry;
        }
    }


    /**
     * Remove a CacheEntry from linked list
     */
    protected void removeEntry(CacheEntry<V> entry) {
        if (entry == _first) {
            _first = entry.getNext();
        }
        if (_last == entry) {
            _last = entry.getPrevious();
        }
        CacheEntry<V> previous = entry.getPrevious();
        CacheEntry<V> next = entry.getNext();
        if (previous != null) {
            previous.setNext(next);
        }
        if (next != null) {
            next.setPrevious(previous);
        }
        entry.setPrevious(null);
        entry.setNext(null);
    }

    /**
     * Place entry at the end of linked list -- Most Recently Used
     */
    protected void touchEntry(CacheEntry<V> entry) {
        if (_last == entry) {
            return;
        }
        removeEntry(entry);
        addEntry(entry);
    }

    /**
     * Purge least recently used object from the cache
     *
     * @return recyclable CacheEntry
     */
    protected CacheEntry<V> purgeEntry() throws CacheEvictionException {
        CacheEntry<V> entry = _first;

        // Notify policy listeners first. if any of them throw an
        // eviction exception, then the internal data structure
        // remains untouched.
        for(CachePolicyListener listener:listeners){
            listener.cacheObjectEvicted(entry.getValue());
        }

        removeEntry(entry);
        _hash.remove(entry.getKey());

        entry.setValue(null);
        return entry;
    }



/**
 * State information for cache entries.
 */
static final class CacheEntry<V> {
    private long _key;
    private V _value;

    private CacheEntry<V> _previous;
    private CacheEntry<V> _next;

    CacheEntry(long key, V value) {
        _key = key;
        _value = value;
    }

    long getKey() {
        return _key;
    }

    void setKey(long obj) {
        _key = obj;
    }

    V getValue() {
        return _value;
    }

    void setValue(V obj) {
        _value = obj;
    }

    CacheEntry<V> getPrevious() {
        return _previous;
    }

    void setPrevious(CacheEntry<V> entry) {
        _previous = entry;
    }

    CacheEntry<V> getNext() {
        return _next;
    }

    void setNext(CacheEntry<V> entry) {
        _next = entry;
    }
}

/**
 * Enumeration wrapper to return actual user objects instead of
 * CacheEntries.
 */
static final class MRUIterator<V> implements Iterator<V> {
    Iterator<CacheEntry<V>> _enum;

    MRUIterator(Iterator<CacheEntry<V>> enume) {
        _enum = enume;
    }

    public boolean hasNext() {
        return _enum.hasNext();
    }

    public V next() {
       return _enum.next().getValue();
    }

	public void remove() {
		throw new UnsupportedOperationException();
	}
}

}