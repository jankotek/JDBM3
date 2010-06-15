package jdbm;

import java.util.Comparator;
import java.util.Map;

import jdbm.helper.JdbmBase;

/**
 * Primary Map which persist data in storage. 
 * Behavior is very similar to  <code>java.util.HashMap/code>.
 * PrimaryMaps have some additional methods to create secondary views. 
 * <p>
 *  
 * @author Jan Kotek
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface PrimaryMap<K,V> extends JdbmBase<K,V>, Map<K,V>{

	/**
	 * Secondary hash view over this PrimaryMap. 
	 * It is readonly, is auto updated as PrimaryMaps is modified. 
	 * View is indexed and persisted, so lookup is always fast
	 *  
	 * @param <A> type of secondary key
	 * @param objectName under this name view will be stored in storage
	 * @param secondaryKeyExtractor extracts secondary key from primary map
	 * @return secondary map 
	 */
	<A> SecondaryHashMap<A,K,V> secondaryHashMap(String objectName, 
			SecondaryKeyExtractor<A,K,V> secondaryKeyExtractor);

	<A> SecondaryHashMap<A,K,V> secondaryHashMapManyToOne(String objectName, 
			SecondaryKeyExtractor<Iterable<A>,K,V> secondaryKeyExtractor);


	<A> SecondaryTreeMap<A,K,V> secondaryTreeMap(String objectName, 
			SecondaryKeyExtractor<A,K,V> secondaryKeyExtractor,Comparator<A> secondaryKeyComparator);

	@SuppressWarnings("unchecked")
	<A extends Comparable> SecondaryTreeMap<A,K,V> secondaryTreeMap(String objectName, 
			SecondaryKeyExtractor<A,K,V> secondaryKeyExtractor);

	<A> SecondaryTreeMap<A,K,V> secondaryTreeMapManyToOne(String objectName, 
			SecondaryKeyExtractor<Iterable<A>,K,V> secondaryKeyExtractor,Comparator<A> secondaryKeyComparator);

	@SuppressWarnings("unchecked")
	<A extends Comparable> SecondaryTreeMap<A,K,V> secondaryTreeMapManyToOne(String objectName, 
			SecondaryKeyExtractor<Iterable<A>,K,V> secondaryKeyExtractor);

	InverseHashView<K,V> inverseHashView(String string);

}
