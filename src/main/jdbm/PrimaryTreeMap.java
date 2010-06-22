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
package jdbm;

import java.util.SortedMap;

/**
 * Primary HashMap which persist data in storage.  
 * Behavior is very similar to  <code>java.util.HashMap/code>, this map also uses b-tree index to lookup keys
 * But it adds some methods to create secondary views
 * <p>
 * Performance note: keys and values are stored as part of index nodes. 
 * They are deserialized on each index lookup. 
 * This may lead to performance degradation and OutOfMemoryExceptions.  
 * If your values are big (>500 bytes) you may consider using <code>PrimaryStoreMap</code>
 * or <code<StoreReference</code> to minimalize size of index.
 *  
 * @author Jan Kotek
 *
 * @param <K> key type
 * @param <V> value type
 */

public interface PrimaryTreeMap<K,V>  extends PrimaryMap<K,V>, SortedMap<K,V>{
	
	/**
	 * In case primary key is Long, new unique Key is generated, otherwise exception is thrown 
	 * @param <K>
	 */
	Long newLongKey();
	
	/**
	 * In case primary key is Integer, new unique Key is generated, otherwise exception is thrown 
	 * @param <K>
	 */
	Integer newIntegerKey();


}
