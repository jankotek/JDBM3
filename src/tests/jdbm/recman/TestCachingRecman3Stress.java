package jdbm.recman;

import java.io.IOException;
import java.util.*;

import jdbm.RecordManager;
import jdbm.Serializer;
import jdbm.helper.RecordManagerImpl;
import junit.framework.TestCase;

public class TestCachingRecman3Stress extends TestCase {

	public volatile int notReady = 0;
	public volatile int notFinished = 0;
	public volatile int uncachedReads = 0;
	public volatile int reads = 0;
	public volatile int exclusiveOperations = 0;
	public Object notifyReady = new Object();
	public Object startingGate = new Object();
	volatile long runto;
	List<Throwable> ex = Collections.synchronizedList(new ArrayList<Throwable>());

	public static final int THREADS = 100;
	public static final long TIME = 10 * 1000L;
	public static final long TIMEOUT = TIME/2;

	public void testRecman() throws Throwable {
		System.err.println("start stress test");

		ThreadGroup tg = new ThreadGroup("Stress test") {
			public void uncaughtException(Thread t, Throwable e) {
				e.printStackTrace();
				ex.add(e);
			}
		};

		final CacheRecordManager3 cc = new CacheRecordManager3(new Stub(), 1000);

		synchronized (notifyReady) {
			System.err.println("Initializing threads");
			for (int i = 0; i < THREADS; i++) {
				Thread t = new Thread(tg, new Runner(cc));
				notReady++;
				notFinished++;
				t.start();
			}

			System.err.println("Waiting for threads to be ready to go");
			while (notReady != 0) {
				notifyReady.wait();
			}

			System.err.println("Launching threads");
			synchronized (startingGate) {
				startingGate.notifyAll();
				runto = System.currentTimeMillis() + TIME; // 20 seconds
			}

			System.err.println("Waiting for threads to finish");

			while (notFinished != 0) {
				notifyReady.wait(System.currentTimeMillis() > runto ? TIMEOUT / 100 : TIME / 4);
				System.err.println((runto - System.currentTimeMillis()) / 1000 + " sec remaining. " + notFinished
						+ " threads not finished");
				System.err.println(reads + " fetches, " + uncachedReads + " uncached fetches");

				if (System.currentTimeMillis() > runto + TIMEOUT) {
					tg.interrupt();
					System.err.println("Overtime. Interrupting threads.");
				}
				else if (System.currentTimeMillis() > runto + TIMEOUT * 2) {
					tg.interrupt();
					System.err.println("Overtime. Interrupting threads and quitting.");
					break;
				}
			}

			System.err.println("All done. " + reads + " reads, " + uncachedReads + " fetches, " + exclusiveOperations
					+ " exclusive operations. " + (100f - ((float) uncachedReads / (float) reads) * 100f)
					+ "% efficiency");
		}

		if (!ex.isEmpty()) {
			throw ex.get(0);
		}

	}

	class Runner implements Runnable {
		final RecordManager recman;
		Random rnd = new Random();

		Runner(RecordManager recman) {
			this.recman = recman;
		}

		public void run() {
			try {
				synchronized (startingGate) {
					synchronized (notifyReady) {
						notReady--;
						if (notReady == 0) {
							notifyReady.notify();
						}
					}
					try {
						startingGate.wait();
					}
					catch (InterruptedException e) {}
				}

				try {
					while (System.currentTimeMillis() < runto) {
						if (rnd.nextInt(1000) == 0) {
							if (rnd.nextBoolean())
								recman.clearCache();
							else
								recman.defrag();
						}
						else {
							int i = rnd.nextInt(50);
							recman.fetch(i);
							reads++;
						}
					}
				}
				catch (RuntimeException ex) {
					throw ex;
				}
				catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			}
			finally {
				synchronized (notifyReady) {
					notFinished--;
					if (notFinished == 0) {
						notifyReady.notify();
					}
				}
			}

		}
	}

	class Stub extends RecordManagerImpl {
		Random rnd = new Random();

		public void close() throws IOException {

		}

		public void commit() throws IOException {}

		public void defrag() throws IOException {
			try {
				long ss;
				synchronized (rnd) {
					ss = rnd.nextInt(200) + 100;
				}
				Thread.sleep(ss);
			}
			catch (InterruptedException e) {
				throw new IOException(e);
			}
			exclusiveOperations++;
		}

		public void clearCache() throws IOException {
			try {
				long ss;
				synchronized (rnd) {
					ss = rnd.nextInt(200) + 100;
				}
				Thread.sleep(ss);
			}
			catch (InterruptedException e) {
				throw new IOException(e);
			}
			exclusiveOperations++;
		}

		public void delete(long recid) throws IOException {}

		public <A> A fetch(long recid, Serializer<A> serializer) throws IOException {
			try {
				long ss;
				synchronized (rnd) {
					ss = rnd.nextInt(50) + 20;
				}
				Thread.sleep(ss);
			}
			catch (InterruptedException e) {
				throw new IOException(e);
			}
			uncachedReads++;
			return null;
		}

		public <A> A fetch(long recid, Serializer<A> serializer, boolean disableCache) throws IOException {
			try {
				long ss;
				synchronized (rnd) {
					ss = rnd.nextInt(50) + 20;
				}
				Thread.sleep(ss);
			}
			catch (InterruptedException e) {
				throw new IOException(e);
			}
			uncachedReads++;
			return null;
		}

		public long getNamedObject(String name) throws IOException {
			return 0;
		}

		public <A> long insert(A obj, Serializer<A> serializer) throws IOException {
			return 0;
		}

		public void rollback() throws IOException {

		}

		public void setNamedObject(String name, long recid) throws IOException {

		}

		public <A> void update(long recid, A obj, Serializer<A> serializer) throws IOException {

		}
	}
}
