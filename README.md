JDBM provides HashMap, TreeMap, HashSet, TreeSet and LinkedList backed up by disk storage.
Now you can insert more than a billion records into collection without running out of memory.
JDBM is probably the fastest pure Java database.

JDBM is tiny with its standalone jar only taking 160 KB (80KB if stripped). Yet it is packed with
impressive features such as: space effective serialization, transactions, MRU/soft/weak instance cache,
defragmentation, transparent encryption and compression.

JDBM has outstanding performance; it can insert a million records per second and read them even faster.
Unlike most of its competitors, it keeps high performance even with a huge number of records (1e12).
Our tests show that it is 4 times faster than BerkleyDB Java Edition and only 2 times slower than
Tokyo Cabinet (probably the fastest db written in C++).

Its performance is a side effect of the author's obsession with minimal overhead.
JDBM runs great even on systems with limited memory and poor Garbage Collector (Android).
Data are read directly without copying, simply by using mapped memory buffers.
Most of the logic is implemented using primitive numbers and arrays. JDBM hardly ever creates new object
instances for its internal use. It also uses advanced techniques to minimise consumed disk space.

Last but not least, JDBM is opensource and free-as-beer under Apache 2.0 license.

News
====
18th Jan 2012 - Alpha1 released, [announcement](http://kotek.net/blog/jdbm_3.0_alpha_1_released) and 
[binary jar](https://github.com/downloads/jankotek/JDBM3/JDBM-3.0-alpha-1.jar)


10 seconds intro
================
	import net.kotek.jdbm.*;

	/** create (or open existing) database using builder pattern*/
	String fileName = "helloWorld";
	DB db = new DBMaker(fileName).build();

	/** Creates TreeMap which stores data in database. */
	SortedMap<Integer,String> map = db.createTreeMap("mapName");

	/** add some stuff to map*/
	map.put(1, "One");
	map.put(2, "Two");
	map.put(3, "Three");

	System.out.println(map.keySet());    // > [1, 2, 3]

	/** Map changes are not persisted yet, commit them (save to disk) */
	db.commit();

	System.out.println(map.keySet());   // > [1, 2, 3]

	/** Delete one record. Changes are not commited yet, but are visible. */
	map.remove(2);

	System.out.println(map.keySet());   // > [1, 3]

	/** Did not like the change. Rollback to last commit (undo record remove). */
	db.rollback();

	/** Key 2 was recovered */
	System.out.println(map.keySet());   // > [1, 2, 3]

	/** close record manager */
	db.close();


10 minutes intro
================

JDBM package exports only two important classes: `DBMaker` is builder which configures and opens database. `DB` is database itself, it creates and opens collections, and controls transactions. Other public classes are just utilities which you may find usefull. Collections in JDBM mimic their `java.util` counter parts, so TreeMap uses an on-disk ordered auto-balanced B*Tree index, LinkedList is stored as self referencing entries and so on. DB and collections should be thread safe (currently under testing).
 
Instance cache
--------------
JDBM caches created instances similar way as Hibernate or other ORM frameworks. This greatly reduces serialization overhead and speedups database. There are five cache types, each configurable with method on `DBMaker` builder:

*  **Most Recently Used** (MRU) cache. It has fixed size and stores newest entries. This cache is on by default and is recommended for systems with limited memory. You can configure its size, default size is 2048.

*  **No cache**. You may disable instance cache by using `DBMaker.disableCache()`

*  **Weak reference cache**. Instances are referenced using `WeakReference`. When item is no longer referenced by other instances, it can be discarded by GC. Use `DBMaker.enableWeakCache()` to enable it.

*  **Soft reference cache**. Instances are referenced using `SoftReference`. Similar to `WeakReference` but holds longer, until systems starts running out of memory. Use `DBMaker.enableSoftCache()` to enable it.

*  **Hard reference cache**. All instances fetched by JDBM are stored in cache until released. GC has no power to discard them.  Use `DBMaker.enableHardCache()` to enable it.

MRU cache is on by default. If you have enought memory (>256M) use Hard reference cache as it has smallest overhead (faster GC and no reference queue). 

With Weak/Soft/Hard cache JDBM checks memory usage every 10 seconds and if free memory is bellow 25%, it clears cache. Our tests shows that GC may not be fast enought to prevent out of memory exceptions. You may also clear cache manually using `DB.clearCache()`, when moving from one type of data to other.

Transactions
------------
JDBM supports single transaction per store. It does not have multiple concurrent transactions with row/table locks, pessimistic locking and similar stuff. This trade off greatly simplifies design and speeds up operations.

Transaction implementation is sound and solid. Uncommited data are stored in memory. During commit, data are appended to end of transaction log file. It is safe, as append operation hardly ever corrupts file. After commit is finished, data are replayed from transaction log file into main storage file. If users calls rollback, transaction log file is discarded.

Keeping transaction log file brings some overhead. It is possible to disable transaction and write changes directly into main storage file. In this case no effort is made to protect file from corruption, all is sacrificed for maximal speed. It is absolutely necessary to properly close storage before exit. You may disable transactions by using `DBMaker.disableTransactions()`.

Uncommited data are stored in memory and flushed to disk during commit. So with large transactions you may run out of memory easily. With disabled transactions data are stored in 10 MB memory buffer and flushed to main storage file when buffer is filled.


Serialization
-------------
JDBM has its own space-efficient serialization which tries to mimic standard implementation. It mimics java serialization, so you still have to implement `Serializable` interface. It also handles cyclic references and some other advanced stuff.

JDBM has custom serialization code for most classes in `java.lang` and `java.util` packages. For `Date` JDBM writes only 9 bytes: 1-byte-long serialization header and 8-byte-long timestamp. For `true`, `String("")` or `Long(3)` JDBM writes only single-byte serialization header. For array list and other collections JDBM writes serialization header, packed size and data. Custom serializers have maximal space efficiency and low overhead.

Standard java serialization stores class structure data (field names, types...) with record data. This generates huge overhead which multiplies with number of records. JDBM serialization stores class structure data in single space and record data only contains reference. So space overhead with POJOs is typically only 3 bytes per class + 1 byte for each field. 

Our serialization is designed to be very fast on small chunks of data (a few POJOs glued together). With couple of thousands nodes in object tree it  becomes slow. This affects single key or value only, and does not apply to JDBM collections. Maximal record size in JDBM is 8 MB anyway, so it is good practise to store only small key/value and use filesystem for larger data.

Troubleshooting
===============
JDBM uses chained exception so user does not have to write try catch blocks. IOException is usually wrapped in IOError which is unchecked. So please always check first exception.

**OutOfMemoryError**
JDBM keeps uncommited data in memory, so you may need to commit more often. If your memory is limited use MRU cache (on by default). You may increase heap size by starting JVM with extra parameter `-Xmx500MB`.

**OutOfMemoryError: GC overhead limit exceeded**
Your app is creating new object instances faster then GC can collect them. When using Soft/Weak cache use Hard cache to reduce GC overhead (is auto cleared when free memory is low). There is JVM parameter to disable this assertion.

**OverlappingFileLockException**
You are trying to open file already opened by another JDBM. Make sure that you `DB.close()` store correctly, operating system may leave lock after JVM is terminated. You may try `DBMaker.useRandomAccessFile()` which is slower, but does not use such aggressive locking. In read-only mode you can also open store multiple times.

**InternalError, Error, AssertionFailedError, IllegalArgumentException, StackOverflowError and so on**
There was an problem in JDBM. It is possible that file store was corrupted thanks to an internal error or disk failure. Disabling cache by `DBMaker.disableCache()` may workaround the problem. Please submit bug report to github. 

---
Special thanks to EJ-Technologies for donating us excellent
[JProfiler](http://www.ej-technologies.com/products/overview.html)




