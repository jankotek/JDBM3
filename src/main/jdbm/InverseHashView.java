package jdbm;

/**
 * Provides inverse view on persisted map.
 * It uses hash index to find Key which belongs to Value. Value must correctly implement hashCode .
 * Internally is backed by SecondaryTreeMap which uses value hashCode as Secondary key.  
 * 
 * @author Jan Kotek
 *
 * @param <K>
 * @param <V>
 */
public interface InverseHashView<K, V>{

	/**
	 * Finds first primary key which corresponds to value. There may be more then one, others are ignored
	 * @param val 
	 * @return first primary key found or null if not found
	 */
	K findKeyForValue(V val);
	
	/**
	 * Finds primary keys which corresponds to value. There may be more then one, others are ignored
	 * @param val 
	 * @return  primary keys found 
	 */
	Iterable<K> findKeysForValue(V val);

	
}
