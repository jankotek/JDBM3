package jdbm.helper;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 
 * Soft reference cache. It does not reference data directly, 
 * rather it references unrelated Object() and removes data object in ReferenceQueue 
 * This way is more predictable then usual SoftHashMap
 * 
 * @author Jan Kotek
 *
 * @param <V>
 */
public class SoftCache<V> implements CachePolicy<V>{


	/** objects in cache */
	protected LongKeyChainedHashMap<CacheEntry> values = 
		new LongKeyChainedHashMap<CacheEntry>();

	
	protected ReferenceQueue<Object> finalizeQueue = new ReferenceQueue<Object>();
	
    /**
     * Cache eviction listeners
     */
    protected List<CachePolicyListener> listeners = new ArrayList<CachePolicyListener>();
    
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


	public Iterator<V> elements() {
		try {
			finalizeItems();
		} catch (CacheEvictionException e) {
			throw new RuntimeException(e);
		}		
		
		final Iterator<CacheEntry> i = values.values().iterator();
		return new Iterator<V>(){

			public boolean hasNext() {
				return i.hasNext();
			}

			public V next() {
				CacheEntry e = i.next();

				return e.value;
			}

			public void remove() {
				throw new UnsupportedOperationException();				
			}};
		
	}

	public V get(long key) {
		try {
			finalizeItems();
		} catch (CacheEvictionException e) {
			throw new RuntimeException(e);
		}
		
		CacheEntry e = values.get(key);
		if(e == null)
			return null;
	
		return e.value;
	}

	public void put(long key, V value) throws CacheEvictionException {
		
		finalizeItems();
		CacheEntry e = new CacheEntry(finalizeQueue,key,value);
		values.put(key, e);
	}

	public void remove(long key) {
		try {
			finalizeItems();
		} catch (CacheEvictionException e) {
			throw new RuntimeException(e);
		}

		values.remove(key);
	}

	public void removeAll() {
		values.clear();
		finalizeQueue = new ReferenceQueue<Object>();
	}
	
	protected void finalizeItems() throws CacheEvictionException{
		CacheEntry e = (CacheEntry)finalizeQueue.poll();
        while ( e!=null) {

	        CacheEntry val2 = values.remove(e.key);
	        //extra check if listeners should be notified
	        boolean notify = val2.value == e.value;


	        
	        // Notify policy listeners first. if any of them throw an
	        // eviction exception, then the internal data structure
	        // remains untouched.
	        if(notify)
	        	for(CachePolicyListener listener:listeners){
	        		listener.cacheObjectEvicted(e.value);
	        	}
	        
	        e.value = null; //speed up GC
	        val2.value = null;
	        
	        e = (CacheEntry)finalizeQueue.poll();
		}
	}
	
	final class CacheEntry extends SoftReference<Object>{
		final long key;
		V value;
		
		public CacheEntry(ReferenceQueue<Object> q, long key, V value) {
			super(new Object(),q);
			this.key = key;
			this.value = value;
		}
		
	}
	
	

}
