package jdbm.helper;

import java.util.Arrays;

/**
 * A very basic thread-safe FIFO ring. The purpose of this class is to hold hard references to something to keep the GC
 * from collecting them. No particular guarantees are made. Specifically, we do not check for duplicates over the entire
 * ring, although we do check the immediately previous insertion (debouncing).
 * 
 * @author <a href="http://paulmurraywork.wordpress.com">Paul Murray</a>
 * 
 * @param <V>
 *            The objects to be put in the mru
 */
public final class MostRecentlyUsed<V> {
	private final int maxIdx; // size - 1.
	protected final V[] mru;
	protected volatile int idx = 0;

	/**
	 * Simple constructor.
	 * @param size number of items in the buffer
	 */
	@SuppressWarnings("unchecked")
	public MostRecentlyUsed(int size) {
		if (size <= 0) throw new IllegalArgumentException("MostRecentlyUsed(" + size + ")");
		this.maxIdx = size - 1;
		this.mru = (V[]) new Object[size];
	}

	/**
	 * Hold a hard reference to some object, for an unspecified amount of time. 
	 * @param v the reference
	 */
	public void add(V v) {
		if (v == null) return;
		synchronized (mru) {
			if (v == mru[idx]) return; // this debounces the input
			if (--idx < 0) idx = maxIdx;
			mru[idx] = v;
		}
	}

	/**
	 * Clear all references.
	 */
	public void clear() {
		synchronized (mru) {
			Arrays.fill(mru, null);
		}
	}
}
