package jdbm;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import jdbm.helper.JdbmBase;

/**
 * Primary HashMap which stores data in storage 
 * 
 * @author Jan Kotek
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface PrimaryHashMap<K,V> extends JdbmBase<K,V>, Map<K,V>{
	
	<A> SecondaryHashMap<A,K,V> secondaryHashMap(String objectName, 
					SecondaryKeyExtractor<A,K,V> secondaryKeyExtractor);
	
	<A> SecondaryHashMap<A,K,V> secondaryHashMapManyToOne(String objectName, 
			SecondaryKeyExtractor<List<A>,K,V> secondaryKeyExtractor);


	<A> SecondaryTreeMap<A,K,V> secondaryTreeMap(String objectName, 
			SecondaryKeyExtractor<A,K,V> secondaryKeyExtractor,Comparator<A> secondaryKeyComparator);
	
	@SuppressWarnings("unchecked")
	<A extends Comparable> SecondaryTreeMap<A,K,V> secondaryTreeMap(String objectName, 
			SecondaryKeyExtractor<A,K,V> secondaryKeyExtractor);

	<A> SecondaryTreeMap<A,K,V> secondaryTreeMapManyToOne(String objectName, 
			SecondaryKeyExtractor<List<A>,K,V> secondaryKeyExtractor,Comparator<A> secondaryKeyComparator);
	
	@SuppressWarnings("unchecked")
	<A extends Comparable> SecondaryTreeMap<A,K,V> secondaryTreeMapManyToOne(String objectName, 
			SecondaryKeyExtractor<List<A>,K,V> secondaryKeyExtractor);


}
