package jdbm.helper;

import java.io.IOException;

import jdbm.RecordListener;
import jdbm.RecordManager;

/**
 * common interface for Trees and PrimaryMaps
 * 
 * @author Jan Kotek
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface JdbmBase<K,V> {

	/**  
	 * @return underlying record manager 
	 */
	RecordManager getRecordManager();
	
    /**
     * add RecordListener which is notified about record changes
     * @param listener
     */
    void addRecordListener(RecordListener<K,V> listener);
    
    /**
     * remove RecordListener which is notified about record changes
     * @param listener
     */
    void removeRecordListener(RecordListener<K,V> listener);
    
    /**
     * Find Value for given Key
     * @param k key
     * @return value or null if not found
     * @throws IOException
     */
    V find(K k) throws IOException;
    
}
