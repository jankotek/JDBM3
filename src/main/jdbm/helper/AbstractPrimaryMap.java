package jdbm.helper;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Comparator;


import jdbm.InverseHashView;
import jdbm.PrimaryHashMap;
import jdbm.SecondaryHashMap;
import jdbm.SecondaryKeyExtractor;
import jdbm.SecondaryTreeMap;

public abstract class AbstractPrimaryMap<K, V> extends AbstractMap<K,V> implements PrimaryHashMap<K, V>{

	
	public <A> SecondaryHashMap<A, K,V> secondaryHashMap(String objectName,
			SecondaryKeyExtractor<A, K,V> secondaryKeyExtractor) {
		return SecondaryKeyHelper.secondaryHashMap(objectName,secondaryKeyExtractor,this);
	}

	public <A> SecondaryTreeMap<A, K, V> secondaryTreeMap(String objectName,
			SecondaryKeyExtractor<A, K, V> secondaryKeyExtractor,
			Comparator<A> secondaryKeyComparator) {
		return SecondaryKeyHelper.secondaryTreeMap(objectName,secondaryKeyExtractor,secondaryKeyComparator,this);	
	}

	@SuppressWarnings("unchecked")
	public <A extends Comparable> SecondaryTreeMap<A, K, V> secondaryTreeMap(
			String objectName, SecondaryKeyExtractor<A, K, V> secondaryKeyExtractor) {
		return SecondaryKeyHelper.secondaryTreeMap(objectName,secondaryKeyExtractor,
				ComparableComparator.INSTANCE,this);
	}

	
	public <A> SecondaryHashMap<A, K,V> secondaryHashMapManyToOne(String objectName,
			SecondaryKeyExtractor<Iterable<A>, K,V> secondaryKeyExtractor) {
		return SecondaryKeyHelper.secondaryHashMapManyToOne(objectName,secondaryKeyExtractor,this);
	}

	public <A> SecondaryTreeMap<A, K, V> secondaryTreeMapManyToOne(String objectName,
			SecondaryKeyExtractor<Iterable<A>, K, V> secondaryKeyExtractor,
			Comparator<A> secondaryKeyComparator) {
		return SecondaryKeyHelper.secondarySortedMapManyToOne(objectName,secondaryKeyExtractor,secondaryKeyComparator,this);	
	}
 
	@SuppressWarnings("unchecked")
	public <A extends Comparable> SecondaryTreeMap<A, K, V> secondaryTreeMapManyToOne(
			String objectName, SecondaryKeyExtractor<Iterable<A>, K, V> secondaryKeyExtractor) {
		return SecondaryKeyHelper.secondarySortedMapManyToOne(objectName,secondaryKeyExtractor,
				ComparableComparator.INSTANCE,this);
	}
	
	public InverseHashView<K, V> inverseHashView(String objectName) {
		return SecondaryKeyHelper.inverseHashView(this,objectName);
	}

	public V find(K k) throws IOException {
		return get(k);
	}

}
