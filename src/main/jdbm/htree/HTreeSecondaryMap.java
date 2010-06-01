package jdbm.htree;

import java.io.IOError;
import java.io.IOException;

import jdbm.SecondaryHashMap;
import jdbm.helper.JdbmBase;

public class HTreeSecondaryMap<A,K,V> extends HTreeMap<A,Iterable<K>> implements SecondaryHashMap<A,K,V>{

	protected final JdbmBase<K,V > b;
	public HTreeSecondaryMap(HTree<A, Iterable<K>> tree, JdbmBase<K,V> b) {
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
