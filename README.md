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
----
18th Jan 2012 - Alpha1 released, [announcement](http://kotek.net/blog/jdbm_3.0_alpha_1_released) and 
[binary jar](https://github.com/downloads/jankotek/JDBM3/JDBM-3.0-alpha-1.jar)


Usage example
-------------
                import net.kotek.jdbm.*;

                /** create (or open existing) database using builder pattern*/
                String fileName = "helloWorld";
                DB db = new DBMaker(fileName)
                        .enableSoftCache()
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

                /** Delete one record. Changes are not commited yet, but are visible. */
                map.remove(2);

                System.out.println(map.keySet());   // > [1, 3]

                /** Did not like the change. Rollback to last commit (undo record remove). */
                db.rollback();

                /** Key 2 was recovered */
                System.out.println(map.keySet());   // > [1, 2, 3]

                /** close record manager */
                db.close();


Note: Special thanks to EJ-Technologies for donating us excellent
[JProfiler](http://www.ej-technologies.com/products/overview.html)




