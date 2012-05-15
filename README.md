JDBM provides TreeMap, HashMap and other collections backed up by disk storage.
Now you can handle billions of items without ever running out of memory.
JDBM is probably the fastest and the simpliest pure Java database. 

JDBM is tiny (160KB nodeps jar), but packed with features such as transactions, 
instance cache and space efficient serialization.
It also has outstanding performance with 1 million inserts per second and 10 million fetches per second (disk based!!). 
It is tightly optimized and has minimal overhead. 
It scales well from Android phone to multi-terrabyte data sets.

JDBM is opensource and free-as-beer under Apache license. 
There is no catch and no strings attached. 

News
====
30th Apr 2012 - JDBM3 [may soon become part of Apache Foundation](https://groups.google.com/forum/?fromgroups#!topic/jdbm/pb4LWr6qTxM). This will not affect github site, but package may be renamed in a few days (done).

10th Apr 2012 - Alpha3 was just released.  Get [binary jar](https://github.com/downloads/jankotek/JDBM3/JDBM-3.0-alpha3.jar) and [read some notes](http://groups.google.com/group/jdbm/browse_thread/thread/db1f0ed52ce5fb3c)

24th Feb 2012 - Alpha2 released with tons of bugfixes. Get [binary jar](https://github.com/downloads/jankotek/JDBM3/JDBM-3.0-alpha2.jar)

18th Jan 2012 - Alpha1 released, [announcement](http://kotek.net/blog/jdbm_3.0_alpha_1_released) and
[binary jar](https://github.com/downloads/jankotek/JDBM3/JDBM-3.0-alpha-1.jar)

Features
========
* B*Tree with `ConcurrentNavigableMap` interface
  * Very fast for sequential read/write.
  * Small values stored inside tree nodes
  * Small values stored inside tree nodes, large values lazily fetched.
  * Self-balancing, great performance even with 1e12 items.
  * Delta compression on keys
  * Submaps (aka cursors) to view limited collection subsets
  * Custom comparators
* H*Tree with `ConcurrentMap` interface
  * Optimized for random reads/writes
  * Small values stored inside tree nodes, large values lazily fetched.
  * Self-balancing, great performance even with 1e12 items.
* TreeSet and HashSet which uses BTree and HTree without values
* LinkedList, which implements bounded BlockingDeque (not implemented yet)
* Multi code scalability (currently under testing)
  * Everything is thread safe
  * Reads should scale linearly with number of cores (as soon as it fits into cache)
  * All collection implements `Concurrent` interfaces
  * Some multi-core scalability with `ReentrantReadWriteLock`. 
* Instance cache
  * If data fits into cache, reads are almost as fast as in-memory collections.
  * Minimal overhead, works well even with 16MB heap.
  * Scales well into 64GB RAM heaps.
  * Various yet simple tuning options
* Transactions
  * Single transaction per store, avoids concurrent modification stuff
  * Transactions are ACID (with limits for single concurrent transaction)
  * Option to disable transactions for fast inserts/updates
* Low level key-value store
  * Various options for on-disk store (NIO, RAF, locking...)
  * Write performance not affected by store fragmentation
  * In-memory store option
  * Can read data from zip file with reasonable performance
  * Can read data from classpath resource, database is deployable over Java Web Start    
  * Advanced defragmentation
  * Print store statistics
  * Transparent data encryption
  * Only 9 bytes overhead per record (for example BTree node)
* Space efficient serialization
  * Custom code for most `java.util` and `java.lang` classes. For example Long(0) takes only single byte
  * Very small POJO serialization overhead, typically only 3 bytes per class + 1 byte for each field. 
  * Mimic java serialization, fields can be `transient`,  all classes needs to implement `Serializable` interface
  * Supports `Externalizable`  
  * Possible to plug your own `Serializer`
* Performance 
  * Blazing fast 1 million inserts / 10 million reads per second (on my 5GHz machine, but you should get 300000 inserts p.s. easily)
  * Tightly optimized code 
  * Uses NIO stuff you read about, but never see in action.
  * Minimal heap usage, prevents `java.lang.OutOfMemoryError: GC overhead limit`
  * Most logic done with primitives or arrays. Minimal stack usage.



Introduction
============
All classes are contained in package `org.apache..jdbm`. There are only two important classes: `DBMaker` is builder which configures and opens database. `DB` is database itself, it opens collections and controls transactions. Collections in JDBM mimic their `java.util` counter parts. TreeMap uses an on-disk ordered auto-balanced B*Tree index, LinkedList is stored as self referencing entries and so on. Everything should be thread safe (currently under testing).

Maven Dependency
----------------

JDBM is not currently in any Maven repository. TODO: We should have soon custom repo with nightly builds. 

Quick example
-------------

    import org.apache.jdbm.*;

    //Open database using builder pattern. 
    //All options are available with code autocompletion.
    DB db = DBMaker.openFile("test")  
        .deleteAfterClose()
        .enableEncryption("password",false)
        .make();
  
    //open an collection, TreeMap has better performance then HashMap
    SortedMap<Integer,String> map = db.createMap("collectionName");

    map.put(1,"one");
    map.put(2,"two");
    //map.keySet() is now [1,2] even before commit

    db.commit();  //persist changes into disk

    map.put(3,"three");
    //map.keySet() is now [1,2,3] 
    db.rollback(); //revert recent changes
    //map.keySet() is now [1,2] 

    db.close();  

A few quick tricks
------------------
* Disabling transaction increases write performance 6x. Do it by `DBMaker.disableTransactions()`. Do not forget to close store correctly in this case!
* When transactions are enabled all uncommited instances are stored in memory. Make sure you commit on time. It is most common cause of `OutOfMemoryError`.
* JDBM does not try to reclaim unused space after massive delete, you must call `DB.defrag(false)` yourself. 
* TreeMap has usually better performance then HashMap. 
* JDBM uses instance cache with limited size by default. If you have enought memory and large store, use unbounded cache: `DBMaker.enableHardCache()`
* JDBM is optimized for small size records. Sizes: 16 bytes is recommended, 32KB is reasonable maximum, 8MB is hard limit.
* JDBM scales well up to 1e12 records. Batch insert overnight creates multi-terrabyte store.

DBMaker
-------

TODO

DB
--

TODO

Collections
-----------

TODO
 
Instance cache
--------------

JDBM caches created instances similar way as Hibernate or other ORM frameworks. This greatly reduces serialization overhead and speedups database. There are five cache types, each configurable with method on `DBMaker` builder:

*  **Most Recently Used** (MRU) cache. It is fixed size and stores newest entries. This cache is on by default. You can configure its size, default size is 2048. This cache has lowest GC overhead and may be suprisingly faster then other cache types. 

*  **No cache**. You may disable instance cache by using `DBMaker.disableCache()`

*  **Hard reference cache**. All instances fetched by JDBM are stored in cache until released. Good with large memory heaps. `Hard` cache is recommended over `soft` and `weak` as it has smaller overhead.  Use `DBMaker.enableHardCache()` to enable it.

*  **Weak reference cache**. Instances are referenced using `WeakReference`. When item is no longer referenced by other instances, it can be discarded by GC. Use `DBMaker.enableWeakCache()` to enable it.

*  **Soft reference cache**. Instances are referenced using `SoftReference`. Similar to `WeakReference` but holds longer, until systems starts running out of memory. Use `DBMaker.enableSoftCache()` to enable it.


With Weak/Soft/Hard cache JDBM starts backround cleanup thread. It also checks memory usage every 10 seconds, if free memory is bellow 25%, it clears cache. Our tests shows that GC is not fast enought to prevent `OutOfMemoryError`. This may be disabled with `DBMaker.disableCacheAutoClear()`.

You may clear cache manually using `DB.clearCache()`. This is usefull after massive delete, or when you are moving from one type of data to other. 

Transactions
------------

JDBM supports single transaction per store. It does not have multiple concurrent transactions with row/table locks, pessimistic locking and similar stuff. This trade off greatly simplifies design and speeds up operations. Transactions are still 'ACID' but in limited way.

Transaction implementation is sound and solid. Uncommited data are stored in memory. Then during commit appended to end of transaction log file. It is safe, as append operation hardly ever corrupts file. After commit is finished, data are replayed from transaction log file into main storage file. If users calls rollback, transaction log file is discarded.

Keeping transaction log file brings some overhead. It is possible to disable transaction and write changes directly into main storage file. It makes inserts and updates about 6x faster. In this case no effort is made to protect file from corruption, all is sacrificed for maximal speed. It is absolutely necessary to properly close storage before exit. You may disable transactions by using `DBMaker.disableTransactions()`.

Uncommited instances are stored in memory and flushed to disk during commit. So with large transactions you may run out of memory easily. With disabled transactions data are stored in 10 MB memory buffer and flushed to main storage file when buffer is filled.


Serialization
-------------

JDBM has its own space-efficient serialization which tries to mimic standard implementation. All classes must implement `Serializable` interface. You may exclude field from serialization by `transient` keyword. Our serialization also handles cyclic references and some other advanced stuff. You may use your own binary format with `Externalizable` interface or custom `Serializer`.

JDBM has custom serialization code for most classes in `java.lang` and `java.util` packages. For `Date` JDBM writes only 9 bytes: 1-byte-long serialization header and 8-byte-long timestamp. For `true`, `String("")` or `Long(3)` JDBM writes only single-byte serialization header. For array list and other collections JDBM writes serialization header, packed size and data. Custom serializers have maximal space efficiency and low overhead.

Standard java serialization stores class structure data (field names, types...) with record data. This generates huge overhead which multiplies with number of records. JDBM serialization stores class structure data in single space and record data only contains reference. So space overhead for POJOs is typically only 3 bytes per class + 1 byte for each field. 

Our serialization is designed to be very fast on small chunks of data (a few POJOs glued together). With couple of thousands nodes in object tree it becomes slow (N^2 scalability).  Maximal record size in JDBM is 8 MB, so it is good practise to store only small key/value in database. You should always use filesystem for data larger then 500KB.  

Defragmentation
---------------

Store gets fragmented. JDBM is well designed, so this does not slows down write/update/delete operations. But fragmentation slows down read operations as more data needs to be readed from disk. JDBM does not do any sort of magic to reclaim unused data. It relies on user to call `DB.defrag` periodically or after massive update/delete/inserts. 

Defrag can be called at runtime, but `DB.defrag` methods blocks other read/writes until it finishes. Defrag basically recreates copyes data from one store to second store. Then it deletes first store and renames second.  

Defragnentation has two modes controlled by `DB.defrag(boolean fullDefrag)` parameter:

**Quick defrag** is designed to be as fast as possible. It only reclaims unused space (compacts store), but does not reorganize data inside store. It copyes all data from one store to other, without empty spaces between records. It is very fast, limited only by disk sequential write speed. Call it by `DB.defrag(false)`

**Full defrag** is designed to make store as fast as possible. It reorganizes data layout, so nodes from single collection are stored close to each other. This makes future reads from store faster as less data needs to be read. Full defrag is much slower than quick defrag, as it traverses and copies all collections unsequentially. 


Troubleshooting
===============

Please report bug into Github error tracker. There is [mail-group](mailto:jdbm@googlegroups.com) if you would have questions, you may also browse [group archive](http://groups.google.com/group/jdbm).

JDBM uses chained exception so user does not have to write try catch blocks. IOException is usually wrapped in IOError which is unchecked. So please always check first exception.

**OutOfMemoryError**
JDBM keeps uncommited data in memory, so you may need to commit more often. If your memory is limited use MRU cache (on by default). You may increase heap size by starting JVM with extra parameter `-Xmx500MB`.

**OutOfMemoryError: GC overhead limit exceeded**
Your app is creating new object instances faster then GC can collect them. When using Soft/Weak cache use Hard cache to reduce GC overhead (is auto cleared when free memory is low). There is JVM parameter to disable this assertion.

**File locking, OverlappingFileLockException, some IOError**
You are trying to open file already opened by another JDBM. Make sure that you `DB.close()` store correctly, operating system may leave lock after JVM is terminated. You may try `DBMaker.useRandomAccessFile()` which is slower, but does not use such aggressive locking. In read-only mode you can also open store multiple times. You may also disable file locks completely by `DB.disableFileLock()` (at your own risk of course)

**InternalError, Error, AssertionFailedError, IllegalArgumentException, StackOverflowError and so on**
There was an problem in JDBM. It is possible that file store was corrupted thanks to an internal error or disk failure. Disabling cache by `DBMaker.disableCache()` may workaround the problem. Please submit bug report to github. 

---
Special thanks to EJ-Technologies for donating us excellent
[JProfiler](http://www.ej-technologies.com/products/overview.html)





