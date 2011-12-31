package jdbm;

import java.util.*;

/**
 * An interface/abstract class to manage records, which are objects serialized to byte[] on background.
 * <p/>
 * The set of record operations is simple: fetch, insert, update and delete.
 * Each record is identified using a "rowid" and contains a byte[] data block serialized to object.
 * Rowids are returned on inserts and you can store them someplace safe
 * to be able to get  back to them.  Data blocks can be as long as you wish,
 * and may have lengths different from the original when updating.
 * <p/>
 * DB is responsible for handling transactions.
 * JDBM2 supports only single transaction for data store.
 * See <code>commit</code> and <code>roolback</code> methods for more details.
 * <p/>
 * DB is also factory for primary Maps.
 * <p/>
 *
 * @author Jan Kotek
 * @author Alex Boisvert
 * @author Cees de Groot
 */
public interface DB {

    /**
     * Closes the record manager and release resources.
     * Record manager can not be used after it was closed
     */
    void close();

    /**
     * Empty cache. This may be usefull if you need to release memory.
     */
    void clearCache();

    /**
     * Defragments storage, so it consumes less space.
     * This commits any uncommited data.
     */
    void defrag();

    /**
     * Commit (make persistent) all changes since beginning of transaction.
     * JDBM supports only single transaction.
     */
    void commit();

    /**
     * This calculates some database statistics.
     * Mostly what collections are presents and how much space is used.
     *
     * @return statistics contained in string
     */
    String calculateStatistics();

    /**
     * Rollback (cancel) all changes since beginning of transaction.
     * JDBM supports only single transaction.
     * This operations affects all maps created or loaded by this DB.
     */
    void rollback();


    <K, V> Map<K, V> loadHashMap(String name);

    /**
     * Creates or load existing Primary Hash Map which persists data into DB.
     *
     * @param <K>  Key type
     * @param <V>  Value type
     * @param name record name
     * @return
     */
    <K, V> Map<K, V> createHashMap(String name);


    /**
     * Creates or load existing Primary Hash Map which persists data into DB.
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
    <K, V> Map<K, V> createHashMap(String name, Serializer<K> keySerializer, Serializer<V> valueSerializer);

    <K> Set<K> createHashSet(String name);

    <K> Set<K> loadHashSet(String name);

    <K> Set<K> createHashSet(String name, Serializer<K> keySerializer);

    <K, V> SortedMap<K, V> loadTreeMap(String name);

    /**
     * Creates or load existing Primary TreeMap which persists data into DB.
     *
     * @param <K>  Key type
     * @param <V>  Value type
     * @param name record name
     * @return
     */
    <K extends Comparable, V> SortedMap<K, V> createTreeMap(String name);

    /**
     * Creates or load existing TreeMap which persists data into DB.
     *
     * @param <K>             Key type
     * @param <V>             Value type
     * @param name            record name
     * @param keyComparator   Comparator used to sort keys
     * @param keySerializer   Serializer used for keys. This may reduce disk space usage     *
     * @param valueSerializer Serializer used for values. This may reduce disk space usage
     * @return
     */
    <K, V> SortedMap<K, V> createTreeMap(String name,
                                         Comparator<K> keyComparator, Serializer<K> keySerializer, Serializer<V> valueSerializer);

    <K> SortedSet<K> loadTreeSet(String name);

    <K> SortedSet<K> createTreeSet(String name);

    <K> SortedSet<K> createTreeSet(String name, Comparator<K> keyComparator, Serializer<K> keySerializer);

    <K> List<K> createLinkedList(String name);

    <K> List<K> createLinkedList(String name, Serializer<K> serializer);

    <K> List<K> loadLinkedList(String name);

}
