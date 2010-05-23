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

}
