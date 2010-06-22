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

/**
 * 
 * Extract secondary key from record in primary map.
 * 
 * @author Jan Kotek
 *
 * @param <A> Type of secondary key
 * @param <K> Type of primary key
 * @param <V> Type of primary value
 */
public interface SecondaryKeyExtractor<A,K,V> {
	
	/**
	 * Extracts secondary key from primary map
	 * 
	 * @param key key in primary table
	 * @param value value in primary table
	 * @return secondary key or null to skip this record
	 */
	A extractSecondaryKey(K key, V value);

}
