package jdbm;

/**
 * Primary HashMap which persist data in storage.  
 * Behavior is very similar to  <code>java.util.HashMap/code>, this map also uses hash index to lookup keys
 * But it adds some methods to create secondary views
 * <p>
 * Performance note: keys and values are stored as part of index nodes. 
 * They are deserialized on each index lookup. 
 * This may lead to performance degradation and OutOfMemoryExceptions.  
 * If your values are big (>500 bytes) you may consider using <code>PrimaryStoreMap</code>
 * or <code<StoreReference</code> to minimalize size of index.
 *  
 * @author Jan Kotek
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface PrimaryHashMap<K,V> extends PrimaryMap<K,V>{
	

}
