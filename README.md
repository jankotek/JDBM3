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


Weak/Soft/Hard should be used only if you have enough heap memory (-Xmx256M or more), otherwise keep default safe option which is MRU cache. Our tests shows that GC may not be fast enough to prevent OutOfMemoryException with reference cache. So JDBM every 10 seconds checks free memory and if it goes bellow 25%, it completely clears reference cache.

You may also clear cache manually using `DB.clearCache()`, when moving from one type of data to other.

Transactions
------------
JDBM supports single transaction per store. So unlike most other databases, it does not have multiple concurrent transactions with row/table locks, pessimistic locking and similar stuff.
This greatly simplify API and speedups database.

Transaction implementation is sound and solid. Uncommited data are stored in memory. Latter during commit, data are appended to end of transaction log file, it makes it safe as append operation hardly ever corrupts file. After commit is finished, data are replayed from transaction log file into main storage file. If users calls rollback, transaction log file is discarded.

Keeping transaction log file brings some overhead. It is possible to disable transaction and speedup modifications. In this case no effort is made to protect file from corruption, all is sacrificed for maximal speed. It is absolutely necessary to properly close storage before exit. You may disable transactions by using `DBMaker.disableTransactions()`.

Uncommited data are stored in memory and flushed to disk during commit. So with large transactions you may run out of memory easily.




---
Special thanks to EJ-Technologies for donating us excellent
[JProfiler](http://www.ej-technologies.com/products/overview.html)




