package jdbm;

import java.util.SortedMap;

import jdbm.helper.JdbmBase;

/**
 * Primary TreeMap which stores data in storage.
 * This map is sorted and allows range queries.
 *  
 * 
 * @author Jan Kotek
 *
 * @param <K> key type
 * @param <V> value type
 *
 */
public interface PrimaryTreeMap<K,V>  extends 
	JdbmBase<K,V>, PrimaryHashMap<K,V>, SortedMap<K,V>{
	
	/**
	 * In case primary key is Long, new unique Key is generated, otherwise exception is thrown 
	 * @param <K>
	 */
	Long newLongKey();
	
	/**
	 * In case primary key is Integer, new unique Key is generated, otherwise exception is thrown 
	 * @param <K>
	 */
	Integer newIntegerKey();


}
