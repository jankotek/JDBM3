package jdbm;

/**
 * Primary map which stores references to storage entries.
 * PrimaryHashMap or PrimaryTreeMap stores keys and values as part of index.    
 * This map stores only <b>record id</b> and values are fetch lazyly. 
 *  
 *  
 * 
 * @author Jan Kotek
 *
 * @param <K> key is record id in storage
 * @param <V> value is lazily fetch record
 */
public interface PrimaryStoreMap<K extends Long,V> extends PrimaryHashMap<Long,V> {
	
	Long putValue(V v);

}
