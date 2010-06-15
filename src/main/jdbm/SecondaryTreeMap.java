package jdbm;

import java.util.SortedMap;

/**
 * Secondary TreeMap. It provides view over primary data. 
 * This map is updated automatically as primary map changes. 
 * This map is unmodifiable, any attempt to modify it will throw 'UnsupportedOperationException'
 * 
 * @author Jan Kotek
 *
 * @param <A> Type of secondary key
 * @param <K> Type of primary key
 * @param <V> Type of value in primary map
 */
public interface SecondaryTreeMap<A,K,V> extends SortedMap<A,Iterable<K>>{

	/**
	 * Convert primary key to primary value. 
	 * This will query primary table and returns result, it is little shortcut.
	 * 
	 * @param k primary key 
	 * @return value from primary table
	 */
	V getPrimaryValue(K k);
	
}
