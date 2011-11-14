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
 * Secondary HashMap. It provides view over primary data. 
 * This map is updated automatically as primary map changes. 
 * This map is unmodifiable, any attempt to modify it will throw 'UnsupportedOperationException'
 * 
 * @author Jan Kotek
 *
 * @param <A> Type of secondary key
 * @param <K> Type of primary key
 * @param <V> Type of value in primary map
 */
public interface SecondaryHashMap<A,K,V> extends Map<A,Iterable<K>>{
	
	/**
	 * Convert primary key to primary value. 
	 * This will query primary table and returns result, it is little shortcut.
	 * 
	 * @param k primary key 
	 * @return value from primary table
	 */
	V getPrimaryValue(K k); 
	
	/**
	 * Returns values from primary map which are matching given secondary key
	 * @param a
	 * @return Iterable over values, this never returns null. 
	 */
	Iterable<V> getPrimaryValues(A a);


}
