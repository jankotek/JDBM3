package jdbm;

/**
 * 
 * Extract secondary key from record in primary map.
 * 
 * @author Jan Kotek
 *
 * @param <A> Type of secondary key
 * @param <K> Type of primary key
 * @param <V> Type of primary value
 */
public interface SecondaryKeyExtractor<A,K,V> {
	
	/**
	 * Extracts secondary key from primary map
	 * 
	 * @param key key in primary table
	 * @param value value in primary table
	 * @return secondary key or null to skip this record
	 */
	A extractSecondaryKey(K key, V value);

}
