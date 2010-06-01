package jdbm;

/**
 * Provides inverse view on persisted map.
 * It fasts finds Key which belongs to Value. Value must correctly implement hashCode .
 * Internally is backed by SecondaryTreeMap which uses value hashCode as Secondary key.  
 * 
 * @author Jan Kotek
 *
 * @param <K>
 * @param <V>
 */
public interface InverseHashView<K, V>{

	/**
	 * Finds primary key which corresponds to value.
	 * @param val 
	 * @return primary key or null if not found
	 */
	K findKeyForValue(V val);
	
}
