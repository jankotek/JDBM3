package jdbm.btree;

import java.io.IOError;
import java.io.IOException;

import jdbm.SecondaryTreeMap;
import jdbm.helper.JdbmBase;

public class BTreeSecondarySortedMap<A,K,V> extends BTreeSortedMap<A,Iterable<K>> 
	implements SecondaryTreeMap<A,K,V>{

	protected final JdbmBase<K,V > b;
	public BTreeSecondarySortedMap(BTree<A, Iterable<K>> tree, JdbmBase<K,V> b) {
		super(tree, true);
		this.b = b;
	}
	
	public V getPrimaryValue(K k) {		
		try {
			return b.find(k);
		} catch (IOException e) {
			throw new IOError(e);
		}
	}

}
