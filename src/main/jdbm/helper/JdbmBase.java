/*******************************************************************************
 * Copyright 2010 Cees De Groot, Alex Boisvert, Jan Kotek
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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
