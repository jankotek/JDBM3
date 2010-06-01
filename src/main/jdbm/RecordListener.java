package jdbm;

import java.io.IOException;

/**
 * An listener notifed when record is inserted, updated or removed 
 * 
 * @author Jan Kotek
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface RecordListener<K,V> {
	
	void recordInserted(K key, V value)throws IOException;
	
	void recordUpdated(K key, V oldValue, V newValue)throws IOException;
	
	void recordRemoved(K key, V value)throws IOException;

}
