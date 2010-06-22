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

import java.io.IOError;
import java.io.IOException;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;

import jdbm.PrimaryStoreMap;
import jdbm.PrimaryTreeMap;
import jdbm.RecordListener;
import jdbm.RecordManager;
import jdbm.Serializer;

public class PrimaryStoreMapImpl<K extends Long, V> extends AbstractPrimaryMap<Long, V>
	implements PrimaryStoreMap<K, V>{
	
	PrimaryTreeMap<Long,Object> map;	
	Serializer<V> valueSerializer;

	public PrimaryStoreMapImpl(PrimaryTreeMap<Long, Object> map,Serializer<V> valueSerializer) {
		this.map = map;
		this.valueSerializer = valueSerializer;
		map.addRecordListener(new RecordListener<Long,Object>(){

			public void recordInserted(Long key,Object value) throws IOException {				
			}

			public void recordRemoved(Long key, Object value) throws IOException {
				//dispose old reference
				getRecordManager().delete(key);
			}

			public void recordUpdated(Long key, Object oldValue, Object newValue) throws IOException {				
				throw new InternalError("should not be here");
			}});
	}

	public Long putValue(V v) {

		try {
			Long recid = getRecordManager().insert(v,valueSerializer);
			map.put(recid, "");
			return recid;
		} catch (IOException e) {
			throw new IOError(e);
		}

	}	

	public void addRecordListener(final RecordListener<Long, V> listener) {
		map.addRecordListener(new RecordListener<Long, Object>() {

			public void recordInserted(Long key, Object value)
					throws IOException {
				listener.recordInserted(key, getRecordManager().fetch(key,valueSerializer));
			}

			public void recordRemoved(Long key, Object value)
					throws IOException {
				listener.recordRemoved(key, getRecordManager().fetch(key,valueSerializer));
			}

			public void recordUpdated(Long key, Object oldValue,
					Object newValue) throws IOException {
				throw new InternalError("Should not happen");
			}
		});
	}

	public RecordManager getRecordManager() {
		return map.getRecordManager();
	}

	public void removeRecordListener(RecordListener<Long, V> listener) {	
		throw new UnsupportedOperationException("not implemented yet");
	}

	public void clear() {
		map.clear();		
	}

	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}


	public Set<java.util.Map.Entry<Long, V>> entrySet() {
		return new AbstractSet<java.util.Map.Entry<Long,V>>(){
			
			protected java.util.Map.Entry<Long,V> newEntry(Long k,V v){
				return new SimpleEntry<Long,V>(k,v){

					public V setValue(V arg0) {
						throw new UnsupportedOperationException();
					};					
				};
			}

			public boolean add(java.util.Map.Entry<Long, V> e) {
				throw new UnsupportedOperationException();
			
			}
			
			@SuppressWarnings("unchecked")
			public boolean contains(Object o) {
				
				if(o instanceof Entry){
					Entry<Long,V> e = (java.util.Map.Entry<Long, V>) o;
					if(e.getKey()!=null && get(e.getKey())!=null)
						return true;
				}
				return false;
			}


			public Iterator<java.util.Map.Entry<Long, V>> iterator() {
					
				return new Iterator<Entry<Long,V>>(){
					final Iterator<Long> keyIter = keySet().iterator();

					public boolean hasNext() {
						return keyIter.hasNext();
					}

					public Entry<Long, V> next() {
						Long k = keyIter.next();
						return newEntry(k, get(k));
					}

					public void remove() {
						keyIter.remove();						
					}
					
				};
					
					
			}

			@SuppressWarnings("unchecked")
			public boolean remove(Object o) {
				
				if(o instanceof Entry){
					Entry<Long,V> e = (java.util.Map.Entry<Long, V>) o;
					
					//check for nulls
					if(e.getKey() == null || e.getValue() == null)
						return false;
					//find old value, must be same as item in entry
					V v = get(e.getKey());
					if(v == null || !e.getValue().equals(v))
						return false;
					return  PrimaryStoreMapImpl.this.remove(e.getKey())!=null;
				}
				return false;

			}



			@SuppressWarnings("unchecked")
			public int size() {
				return PrimaryStoreMapImpl.this.size();
			}

		};
	}

	public V get(Object key) {
		if(!map.containsKey(key))
			return null;
		try {
			return getRecordManager().fetch((Long)key,valueSerializer);
		} catch (IOException e) {
			throw new IOError(e);
		}
	}


	public Set<Long> keySet() {
		return map.keySet();
	}

	public V put(Long key, V value) {
		if(containsKey(key)){
			V oldVal = get(key);
			try {
				getRecordManager().update(key, value);
			} catch (IOException e) {
				throw new IOError(e);
			}
			return oldVal;
		}else{			
			throw new UnsupportedOperationException("Can not update, key not found, use putValue(val) instead.");
		}
	}

	public V remove(Object key) {
		
		if(!map.containsKey(key))
			return null;
		try{
			V v = getRecordManager().fetch((Long)key,valueSerializer);
			map.remove(key);		
			return v;
		}catch (IOException e){
			throw new IOError(e);
		}
	}

	public int size() {
		return map.size();
	}


}
