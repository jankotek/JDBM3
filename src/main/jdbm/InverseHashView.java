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
 * Provides inverse view on persisted map.
 * It uses hash index to find Key which belongs to Value. Value must correctly implement hashCode .
 * Internally is backed by SecondaryTreeMap which uses value hashCode as Secondary key.  
 * 
 * @author Jan Kotek
 *
 * @param <K>
 * @param <V>
 */
public interface InverseHashView<K, V>{

	/**
	 * Finds first primary key which corresponds to value. There may be more then one, others are ignored
	 * @param val 
	 * @return first primary key found or null if not found
	 */
	K findKeyForValue(V val);
	
	/**
	 * Finds primary keys which corresponds to value. There may be more then one, others are ignored
	 * @param val 
	 * @return  primary keys found 
	 */
	Iterable<K> findKeysForValue(V val);

	
}
