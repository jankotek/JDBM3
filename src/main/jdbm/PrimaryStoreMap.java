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

import java.util.Map;

/**
 * Primary map which stores references to storage entries.
 * PrimaryHashMap or PrimaryTreeMap stores keys and values as part of index.    
 * This map stores only <b>record id</b> and values are fetch lazily. 
 *  
 *  
 * 
 * @author Jan Kotek
 *
 * @param <K> key is record id in storage
 * @param <V> value is lazily fetch record
 */
public interface PrimaryStoreMap<K extends Long,V> extends PrimaryMap<Long,V> {
	
	Long putValue(V v);
	

}
