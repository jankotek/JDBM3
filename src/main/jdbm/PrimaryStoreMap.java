package jdbm;

public interface PrimaryStoreMap<K extends Long,V> extends PrimaryHashMap<Long,V> {
	
	Long putValue(V v);

}
