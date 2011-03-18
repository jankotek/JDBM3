package jdbm.recman;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.*;

import jdbm.RecordManager;
import jdbm.Serializer;
import jdbm.helper.*;

/**
 * <p>
 * A RecordManager wrapping and caching another RecordManager.
 * </p>
 * <p>
 * This class does fine-grained synchronisation of an underlying recored manager. That is: loading and mutating a single
 * object blocks only on the cache entry for that object. Global operations - commit, rollback, etc - block on the whole
 * cache and take priority.
 * </p>
 * <p>
 * No special caching is done for named objects. This class assumes that the usual usage pattern for them is that they
 * are persistent handles to objected that are not likely to be volatile.
 * </p>
 * <p>
 * This class assumes that the underlying record manager manages contending writes. In particular, this class does not
 * do lazy writing and permits multiple updates to different records simultaneously.
 * </p>
 * <p>
 * The MRU (Most Recently Used) buffer holds the N most recent accesses, not the N most recent objects. This means that
 * it is not guaranteed that the N most recently used ids will indeed be cached. However, if the JVM aggressively clears
 * SoftReferences (which it ought not to do), using the MRU may improve performance, at the risk of OutOfMemory errors
 * if the items in your repository are large.
 * </p>
 * <p>
 * 
 * @author Paul Murray (<a href="mailto:pmurray@bigpond.com">mail</a>, <a
 *         href="mailto:pmurray_bigpond_com@gmail.com">gmail</a>, <a
 *         href="http://paulmurraywork.wordpress.com/">blog</a>)
 */

public class CacheRecordManager3 extends RecordManagerImpl {
	protected final RecordManager recman;

	protected final WeakHashMap<Long, SoftReference<RefWrapper>> cache = new WeakHashMap<Long, SoftReference<RefWrapper>>();
	protected final LockingStrategy dbLock = new LockingStrategy();
	protected final MostRecentlyUsed<RefWrapper> mru;

	/**
	 * Create a recman that caches some other recman in-memory. Mru is set to 0 - we rely entirely on java being
	 * nonaggressive with softly held objects.
	 * 
	 * We do not have any special tuning parameters at this stage. I rely on the CG.
	 * 
	 * @param recman
	 */

	public CacheRecordManager3(RecordManager recman) {
		this(recman, 0);
	}

	/**
	 * Create a recman that caches some other recman in-memory.
	 * 
	 * @param recman
	 * @param mruSize
	 *            mostRecentlyUsed
	 */

	public CacheRecordManager3(RecordManager recman, int mruSize) {
		this.recman = recman;
		if (mruSize > 0) {
			this.mru = new MostRecentlyUsed<RefWrapper>(mruSize);
		}
		else {
			this.mru = null;
		}
	}

	/**
	 * These objects hold entries from our underlying store. If they are in an "unloaded" state, then the object has not
	 * been fetched from the underlying store.
	 * 
	 * We hold a hard ref to the object, but this wrapper is always held by a soft ref and is collectable. This wrapper
	 * holds a hard reference to the key used in the weak hash map. That means that the key cannot be removed from the
	 * map until the SoftReference holding this object is cleared.
	 */

	protected static class RefWrapper {
		final Long mapKey;

		/**
		 * We do *not* assume that v=null means that the object is unloaded. If the object is loaded, then v=null means
		 * that the key is not present in the underlying store.
		 */
		volatile boolean loaded = false;

		/**
		 * The object in the store, or null if there is no opject with this key.
		 */
		volatile Object v;

		/**
		 * This boolean resolves contention between multiple threads attempting to create/modify/delete the object
		 * referred to by the key.
		 */
		volatile boolean lock = false;

		/**
		 * @param l
		 *            very important - pass in the object, <strong>NOT</strong> the long value. we must hold a hard
		 *            refereence to the same object that is the key in the hash map
		 */

		RefWrapper(Long l) {
			if (l == null) throw new IllegalArgumentException();
			this.mapKey = l;
		}

		/**
		 * Bulletproofing. To re-iterate, passing in a primitive long. This method is here to stop the compiler
		 * auto-boxing your longs.
		 * 
		 * @deprecated do not use this method
		 * @param l
		 *            the object id &hellip; in principle
		 * @throws UnsupportedOperationException
		 *             unconditionally
		 */
		RefWrapper(long l) {
			throw new UnsupportedOperationException();
		}

		/**
		 * Tell this wrapper what object is in the underlying store.
		 * 
		 * @param v
		 *            the object, or null if there is no object with this key.
		 */
		void set(Object v) {
			if (!lock) throw new IllegalStateException();
			this.v = v;
			loaded = true;
		}

		/**
		 * Tell this wrapper to forget about what is in the underlying store
		 */
		void unset() {
			if (!lock) throw new IllegalStateException();
			this.v = null;
			loaded = false;
		}
	}

	/**
	 * Acquire a lock on a particular object id.
	 * 
	 * @param recid
	 *            the record to be locked
	 * @return A hard reference to the "holder" object.
	 * 
	 * @throws InterruptedException
	 *             you must *not* call unlock if this is thrown.
	 */
	protected RefWrapper lock(long recid) throws InterruptedException {
		dbLock.acquireNonexclusiveLock();
		RefWrapper ref;
		synchronized (mutatingTheCache) {
			SoftReference<RefWrapper> refref = cache.get(recid);
			ref = refref == null ? null : refref.get();
			if (ref == null) {
				Long k = new Long(recid);
				ref = new RefWrapper(k);
				cache.put(k, new SoftReference<RefWrapper>(ref));
			}

			if (mru != null) mru.add(ref);
		}
		try {
			synchronized (ref) {
				while (ref.lock) {
					ref.wait();
				}
				ref.lock = true;
			}
		}
		catch (InterruptedException e) {
			// drat. we got the global lock, but not the specific object lock.
			dbLock.releaseNonexclusiveLock();
			throw e;
		}

		return ref;
	}

	/**
	 * Release the lock on a particular entry, notify anyone else waiting on that particular entry, and release a global
	 * non-exclusive lock.
	 * 
	 * @param ref
	 */
	protected void unlock(RefWrapper ref) {
		synchronized (ref) {
			if (!ref.lock) throw new IllegalStateException();
			ref.lock = false;
			ref.notify();
		}

		dbLock.releaseNonexclusiveLock();
	}

	/**
	 * Mutex for non-exclusive threads accessing the cache. Operations on this are fast. In particular, no method talks
	 * to the underlying recman when it is synchronised on this. <br>
	 * (We do not synchronise on this is we have an exclusive lock - it's only for contending entry threads calling
	 * "get lock". We rely on the behaviour of the locking strategy class.)
	 */
	protected final Object mutatingTheCache = new Object();

	public void clearCache() throws IOException {
		try {
			dbLock.acquireExclusiveLock();
		}
		catch (InterruptedException e) {
			throw new IOException("Interrupted", e);
		}

		try {
			if (mru != null) mru.clear();
			cache.clear();
			recman.clearCache();
		}
		finally {
			dbLock.releaseExclusiveLock();
		}
	}

	public void close() throws IOException {
		try {
			dbLock.acquireExclusiveLock();
		}
		catch (InterruptedException e) {
			throw new IOException("Interrupted", e);
		}

		try {
			if (mru != null) mru.clear();
			cache.clear();
			recman.close();
		}
		finally {
			dbLock.releaseExclusiveLock();
		}
	}

	// I could add logic to mark records as dirty - but the underlying
	// manager does this, so I won't double-up on effort

	public void commit() throws IOException {
		try {
			dbLock.acquireExclusiveLock();
		}
		catch (InterruptedException e) {
			throw new IOException("Interrupted", e);
		}

		try {
			// we do not clear the cache.
			recman.commit();
		}
		finally {
			dbLock.releaseExclusiveLock();
		}
	}

	public void rollback() throws IOException {
		try {
			dbLock.acquireExclusiveLock();
		}
		catch (InterruptedException e) {
			throw new IOException("Interrupted", e);
		}

		try {
			if (mru != null) mru.clear();
			cache.clear();
			recman.rollback();
		}
		finally {
			dbLock.releaseExclusiveLock();
		}
	}

	public void defrag() throws IOException {
		try {
			dbLock.acquireExclusiveLock();
		}
		catch (InterruptedException e) {
			throw new IOException("Interrupted", e);
		}

		try {
			// we do not clear the cache.
			recman.defrag();
		}
		finally {
			dbLock.releaseExclusiveLock();
		}
	}

	// I don't do any magic with named objects, but rely on the underlying
	// recman.

	public long getNamedObject(String name) throws IOException {
		try {
			dbLock.acquireNonexclusiveLock();
		}
		catch (InterruptedException e) {
			throw new IOException("Interrupted", e);
		}

		try {
			return recman.getNamedObject(name);
		}
		finally {
			dbLock.releaseNonexclusiveLock();
		}

	}

	public void setNamedObject(String name, long recid) throws IOException {
		try {
			dbLock.acquireNonexclusiveLock();
		}
		catch (InterruptedException e) {
			throw new IOException("Interrupted", e);
		}

		try {
			recman.setNamedObject(name, recid);
		}
		finally {
			dbLock.releaseNonexclusiveLock();
		}
	}

	@SuppressWarnings("unchecked")
	public <A> A fetch(long recid, Serializer<A> serializer) throws IOException {
		RefWrapper ref;

		try {
			ref = lock(recid);
		}
		catch (InterruptedException e) {
			throw new IOException("Interrupted", e);
		}

		try {
			if (!ref.loaded) {
				ref.v = recman.fetch(recid, serializer, false);
				ref.loaded = true;
			}

			return (A) ref.v;
		}
		finally {
			unlock(ref);
		}
	}

	public <A> A fetch(long recid, Serializer<A> serializer, boolean disableCache) throws IOException {
		if (disableCache) {
			return recman.fetch(recid, serializer);
		}
		else {
			return fetch(recid, serializer);
		}
	}

	public <A> long insert(A obj, Serializer<A> serializer) throws IOException {
		try {
			dbLock.acquireNonexclusiveLock();
		}
		catch (InterruptedException e) {
			throw new IOException("Interrupted", e);
		}

		long recId;
		try {
			// we'll do this the simple way. If someone
			// wants this record stright away, a ref wrapper
			// will be created shortly.
			recId = recman.insert(obj, serializer);
			synchronized (mutatingTheCache) {
				cache.remove(recId);
			}
		}
		finally {
			dbLock.releaseNonexclusiveLock();
		}

		return recId;
	}

	public <A> void update(long recid, A obj, Serializer<A> serializer) throws IOException {
		RefWrapper ref;

		try {
			ref = lock(recid);
		}
		catch (InterruptedException e) {
			throw new IOException("Interrupted", e);
		}

		try {
			ref.unset();
			recman.update(recid, obj, serializer);
			ref.set(obj);
		}
		finally {
			unlock(ref);
		}
	}

	public void delete(long recid) throws IOException {
		RefWrapper ref;

		try {
			ref = lock(recid);
		}
		catch (InterruptedException e) {
			throw new IOException("Interrupted", e);
		}

		try {
			ref.unset();
			recman.delete(recid);
			ref.set(null);
		}
		finally {
			unlock(ref);
		}
	}

}