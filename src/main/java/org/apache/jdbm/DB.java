package org.apache.jdbm;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Database is root class for creating and loading persistent collections. It also contains
 * transaction operations.
 * //TODO just write some readme
 * <p/>
 *
 * @author Jan Kotek
 * @author Alex Boisvert
 * @author Cees de Groot
 */
public interface DB {

    /**
     * Closes the DB and release resources.
     * DB can not be used after it was closed
     */
    void close();

    /** @return true if db was already closed */
    boolean isClosed();

    /**
     * Clear cache and remove all entries it contains.
     * This may be useful for some Garbage Collection when reference cache is used.
     */
    void clearCache();

    /**
     * Defragments storage so it consumes less space.
     * It basically copyes all records into different store and then renames it, replacing original store.
     * <p/>
     * Defrag has two steps: In first collections are rearranged, so records in collection are close to each other,
     * and read speed is improved. In second step all records are sequentially transferred, reclaiming all unused space.
     * First step is optinal and may slow down defragmentation significantly as ut requires many random-access reads.
     * Second step reads and writes data sequentially and is very fast, comparable to copying files to new location.
     *
     * <p/>
     * This commits any uncommited data. Defrag also requires free space, as store is basically recreated at new location.
     *
     * @param sortCollections if collection records should be rearranged during defragment, this takes some extra time
     */
    void defrag(boolean sortCollections);

    /**
     * Commit (make persistent) all changes since beginning of transaction.
     * JDBM supports only single transaction.
     */
    void commit();

    /**
     * Rollback (cancel) all changes since beginning of transaction.
     * JDBM supports only single transaction.
     * This operations affects all maps created or loaded by this DB.
     */
    void rollback();

    /**
     * This calculates some database statistics such as collection sizes and record distributions.
     * Can be useful for performance optimalisations and trouble shuting.
     * This method can run for very long time.
     *
     * @return statistics contained in string
     */
    String calculateStatistics();


    /**
     * Copy database content into ZIP file
     * @param zipFile
     */
    void copyToZip(String zipFile);



    /**
     * Get a <code>Map</code> which was already created and saved in DB.
     * This map uses disk based H*Tree and should have similar performance
     * as <code>HashMap</code>.
     *
     * @param name of hash map
     *
     * @return map
     */
    <K, V> ConcurrentMap<K, V> getHashMap(String name);

    /**
     * Creates Map which persists data into DB.
     *
     * @param name record name
     * @return
     */
    <K, V> ConcurrentMap<K, V> createHashMap(String name);


    /**
     * Creates  Hash Map which persists data into DB.
     * Map will use custom serializers for Keys and Values.
     * Leave keySerializer null to use default serializer for keys
     *
     * @param <K>             Key type
     * @param <V>             Value type
     * @param name            record name
     * @param keySerializer   serializer to be used for Keys, leave null to use default serializer
     * @param valueSerializer serializer to be used for Values
     * @return
     */
    <K, V> ConcurrentMap<K, V> createHashMap(String name, Serializer<K> keySerializer, Serializer<V> valueSerializer);

    <K> Set<K> createHashSet(String name);

    <K> Set<K> getHashSet(String name);

    <K> Set<K> createHashSet(String name, Serializer<K> keySerializer);

    <K, V> ConcurrentNavigableMap<K, V> getTreeMap(String name);

    /**
     * Create  TreeMap which persists data into DB.
     *
     * @param <K>  Key type
     * @param <V>  Value type
     * @param name record name
     * @return
     */
    <K extends Comparable, V> NavigableMap<K, V> createTreeMap(String name);

    /**
     * Creates TreeMap which persists data into DB.
     *
     * @param <K>             Key type
     * @param <V>             Value type
     * @param name            record name
     * @param keyComparator   Comparator used to sort keys
     * @param keySerializer   Serializer used for keys. This may reduce disk space usage     *
     * @param valueSerializer Serializer used for values. This may reduce disk space usage
     * @return
     */
    <K, V> ConcurrentNavigableMap<K, V> createTreeMap(String name,
                                         Comparator<K> keyComparator, Serializer<K> keySerializer, Serializer<V> valueSerializer);

    <K> NavigableSet<K> getTreeSet(String name);

    <K> NavigableSet<K> createTreeSet(String name);

    <K> NavigableSet<K> createTreeSet(String name, Comparator<K> keyComparator, Serializer<K> keySerializer);

    <K> List<K> createLinkedList(String name);

    <K> List<K> createLinkedList(String name, Serializer<K> serializer);

    <K> List<K> getLinkedList(String name);

    /** returns unmodifiable map which contains all collection names and collections thenselfs*/
    Map<String,Object> getCollections();

    /** completely remove collection from store*/
    void deleteCollection(String name);

    /** Java Collections returns their size as int. This may not be enought for JDBM collections.
     * This method returns number of elements in JDBM collection as long.
     *
     * @param collection created by JDBM
     * @return number of elements in collection as long
     */
    long collectionSize(Object collection);

}
