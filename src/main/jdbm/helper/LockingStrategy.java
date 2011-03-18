package jdbm.helper;

/**
 * <p>
 * An thread-synchronisation strategy based around the idea of exclusive and non-exclusive locks. Threads may acquire
 * and release exclusive locks and non-exclusive locks.
 * <ul>
 * <ol>
 * <li>When any thread is attempting to acquire an exclusive lock, all other threads attempting to acquire nonexclusive
 * locks are blocked.</li>
 * <li>All threads attempting to acquire any locks block until all non-exclusive locks have been released</li>
 * <li>When no non-exclusive locks remain, all threads attempting to acquire exclusive locks are unblocked
 * <em>one at a time</em></li>
 * <li>
 * When there are no more threads seeking exclusive locks, all threads seeking non-exclusive locks are unblocked.</li>
 * </ol>
 * No guarantees are made with respect to the sequence in which contending threads are unblocked apart from the above.
 * </p>
 * <p>
 * When using this class, <strong>it is important to use finally blocks to make sure that every acquired lock is
 * released</strong>. Additionally, <strong>do not attempt to get two exclusive write locks</strong> - this class does
 * not note the identity of the thread currently holding the exclusive lock and it will block the second attempt
 * indefinitely.
 * </p>
 * 
 * @author <a href="http://paulmurraywork.wordpress.com">Paul Murray</a>
 * 
 */
public final class LockingStrategy {
	/*
	 * Making the class final, for performance. It makes the protected scoping moot, but whatever.
	 */
	/*
	 * It's possible that this code is a little old-school and that I have re-invented the wheel. I do not use the
	 * concurrency library because I am not familiar with it - I grew up in the late 90's, as far as Java is concerned.
	 */

	/**
	 * Number of threads currently reading/writing entries in the cache. We cannot get an exclusive lock while this > 1.
	 * 
	 * <strong>We never access this without synchronising on {@link #waitingForExclusiveLock}</strong>
	 */
	protected volatile int nonexclusiveLocks = 0;
	/**
	 * Number of threads queued up for an exclusive lock - including any thread that currently has one. If we want to
	 * read/write entries in the cache, we must wait until this drops to zero.
	 * 
	 * <strong>We never access this without synchronising on {@link #waitingForNonExcusiveLock}</strong>
	 */
	protected volatile int exclusiveLocks = 0;

	/**
	 * This boolean resolves contention between multiple threads attempting to acquire an exclusive lock.
	 * 
	 * <strong>We never access this without synchronising on {@link #waitingForExclusiveLock}</strong>
	 */
	protected volatile boolean exclusivelyLocked = false;

	/**
	 * Mutex for threads attempting to modify individual entries. It gets a notifyAll() when the last exclusive lock is
	 * released.
	 */
	protected final Object waitingForNonExcusiveLock = new Object();

	/**
	 * Mutex for threads attempting to acquire an exclusive lock. It gets a notify when another thread releases an
	 * exclusive lock, and when the last non-exclusive lock is released.
	 */
	protected final Object waitingForExclusiveLock = new Object();

	/**
	 * Acquire a nonexclusive lock. Blocks until all exclusive locks are released. I recommend that you use a finally
	 * block to ensure that acquired locks are released.
	 * 
	 * @throws InterruptedException
	 *             you must <strong>not</strong> call releaseNonexclusiveLock if this is thrown.
	 */
	public void acquireNonexclusiveLock() throws InterruptedException {
		acquireNonexclusiveLock(-1L);
	}

	/**
	 * Acquire a nonexclusive lock. Blocks until all exclusive locks are released. I recommend that you use a finally
	 * block to ensure that acquired locks are released.
	 * 
	 * @param timeout
	 *            As per {@link java.lang.Object#wait(long) Object.wait(timeout)}
	 * @throws InterruptedException
	 *             you must <strong>not</strong> call releaseNonexclusiveLock if this is thrown.
	 */
	public void acquireNonexclusiveLock(long timeout) throws InterruptedException {
		synchronized (waitingForNonExcusiveLock) {
			while (exclusiveLocks > 0) {
				if (timeout == -1L)
					waitingForNonExcusiveLock.wait();
				else
					waitingForNonExcusiveLock.wait(timeout);
			}
			// ok! no exclusive locks. increment the entry locks

			final int lc = ++nonexclusiveLocks;
			if (lc <= 0) throw new IllegalStateException();
			if (exclusivelyLocked) throw new IllegalStateException();
		}
	}

	/**
	 * Release a nonexclusive lock. Conditionally unblocks threads seeking exclusive locks.
	 */

	public void releaseNonexclusiveLock() {
		if (nonexclusiveLocks <= 0) throw new IllegalStateException();
		if (exclusivelyLocked) throw new IllegalStateException();

		synchronized (waitingForExclusiveLock) {
			// ok, this is a double lock. We know that it will not deadlock because
			// the other place where this appears is locked in the same sequence
			synchronized (waitingForNonExcusiveLock) {
				final int lc = --nonexclusiveLocks;
				if (lc == 0) {
					waitingForExclusiveLock.notify();
				}
			}
		}
	}

	/**
	 * Acquire an exclusive lock. Blocks until no other thread has a lock. I recommend that you use a finally block to
	 * ensure that acquired locks are released.
	 * 
	 * @throws InterruptedException
	 *             you must <strong>not</strong> call releaseExclusiveLock if this is thrown.
	 */

	public void acquireExclusiveLock() throws InterruptedException {
		acquireExclusiveLock(-1L);
	}

	/**
	 * Acquire an exclusive lock. Blocks until no other thread has a lock. I recommend that you use a finally block to
	 * ensure that acquired locks are released.
	 * 
	 * @param timeout
	 *            As per {@link java.lang.Object#wait(long) Object.wait(timeout)}
	 * @throws InterruptedException
	 *             you must <strong>not</strong> call releaseExclusiveLock if this is thrown.
	 * 
	 * @throws InterruptedException
	 */

	public void acquireExclusiveLock(long timeout) throws InterruptedException {
		synchronized (waitingForNonExcusiveLock) {
			exclusiveLocks++;
			// ok. from this point, i know that no-one will increment entry locks
			// so I wait until all the locks are gone.
		}

		synchronized (waitingForExclusiveLock) {
			while (nonexclusiveLocks > 0 || exclusivelyLocked) {
				try {
					waitingForExclusiveLock.wait();
				}
				catch (InterruptedException e) {
					// we can't assert anything about the entry locks, because the
					// entry locking thread may or may not have released all their locks by now.
					if (exclusiveLocks <= 0) throw new IllegalStateException();

					// ok, this is a double lock. We know that it will not deadlock because
					// the other place where this appears is locked in the same sequence
					synchronized (waitingForNonExcusiveLock) {
						exclusiveLocks--;
						if (exclusiveLocks == 0) {
							waitingForNonExcusiveLock.notifyAll();
						}
						else {
							waitingForExclusiveLock.notify();
						}
					}
					throw e;
				}
			}

			// redundant, but I am asserting conditions everywhere else.
			if (exclusiveLocks <= 0) throw new IllegalStateException();
			if (nonexclusiveLocks != 0) throw new IllegalStateException();
			if (exclusivelyLocked) throw new IllegalStateException();

			exclusivelyLocked = true;
		}

		// and now I have an exclusive lock, so I can continue.
	}

	/**
	 * Release the exclusive lock. Unblock one other thread also seeking an exclusive lock, or all other threads seeking
	 * non-exclusive locks.
	 */

	public void releaseExclusiveLock() {
		if (exclusiveLocks <= 0) throw new IllegalStateException();
		if (nonexclusiveLocks != 0) throw new IllegalStateException();
		if (!exclusivelyLocked) throw new IllegalStateException();

		// ok, this is a double lock. We know that it will not deadlock because
		// the other place where this appears is locked in the same sequence
		synchronized (waitingForExclusiveLock) {
			synchronized (waitingForNonExcusiveLock) {
				exclusiveLocks--;
				exclusivelyLocked = false;
				if (exclusiveLocks == 0) {
					waitingForNonExcusiveLock.notifyAll();
				}
				else {
					waitingForExclusiveLock.notify();
				}
			}
		}

		// we do not assert here, because heck - someone might have grabbed a lock
		// as you read this. We are no longer synchronised.
	}

}
