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
public interface SecondaryTreeMap<A,K,V> extends 
		SecondaryHashMap<A,K,V>, SortedMap<A,Iterable<K>>{

}
