JDBM provides HashMap, TreeMap, HashSet, TreeSet and LinkedList backed up by disk storage.
Now you can insert billion records into collection without running out of memory.
This is probably fastest and easiest to use pure Java database.

JDBM has minimal size, standalone jar has only 160 KB (90KB stripped). Yet it is packed with
impressive features such as: space effective serialization, transactions, MRU/soft/weak instance cache,
defragmentation, encryption and compression.

JDBM has outstanding performance, it can insert 1 million records per second and read even faster.
Unlike most competitors it keeps high performance even with huge number of records (1e9).
Our tests shows that it is 4 times faster than BerkleyDB Java Edition and only 2 times slower than
Tokyo Cabinet (probably fastest db written in C++).

Performance is side effect of author's obsession with minimal overhead.
JDBM runs great even on systems with limited memory or poor GC (Android)
Data are read directly using direct mapped buffers without any copying.
Most logic is implemented using primitive numbers and arrays. JDBM hardly ever creates new object
instances for its internal use.

On top of that JDBM offers various deployment modes. You can store data on disk, memory,
zip/jar file. DB can be encrypted using password or quickly backuped to zip file.
Various stores can be also combined, for example DB deployed over webstart with modifications
stored in users folder. JDBM truly is swiss army knife for most of your storage needs.

And best of all JDBM is opensource and free-as-beer under Apache 2.0 license.

Usage example
-------------
                import net.kotek.jdbm.*;

                /** create (or open existing) database using builder pattern*/
                String fileName = "helloWorld";
                DB db = new DBMaker(fileName)
                        .enableSoftCache()
                        .enableEncryption("password")
                        .build();

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

                /** Delete one record. Changes are not comited yet, but are visible. */
                map.remove(2);

                System.out.println(map.keySet());   // > [1, 3]

                /** Did not like the change. Roolback to last commit (undo record remove). */
                db.rollback();

                /** Key 2 was recovered */
                System.out.println(map.keySet());   // > [1, 2, 3]

                /** close record manager */
                recMan.close();


Note: Special thanks to EJ-Technologies for donating us excellent
(http://www.ej-technologies.com/products/overview.html "JProfiler")




