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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import jdbm.PrimaryMap;
import jdbm.PrimaryStoreMap;
import jdbm.RecordListener;
import jdbm.RecordManager;
import jdbm.Serializer;

public class PrimaryStoreMapImpl<K extends Long, V> extends AbstractPrimaryMap<Long, V>
	implements PrimaryStoreMap<K, V>{
	
	final protected PrimaryMap<Long,String> map;	
	final protected Serializer<V> valueSerializer;
	final protected List<RecordListener<Long,V>> listeners = new CopyOnWriteArrayList<RecordListener<Long,V>>();

	public PrimaryStoreMapImpl(PrimaryMap<Long, String> map,Serializer<V> valueSerializer2) {
		this.map = map;
		this.valueSerializer = valueSerializer2;
		map.addRecordListener(new RecordListener<Long,String>(){

			public void recordInserted(Long key,String value) throws IOException {
				V v = (V) getRecordManager().fetch(key,valueSerializer);
				for(RecordListener<Long,V> l:listeners)
					l.recordInserted(key, v);
			}

			public void recordRemoved(Long key, String value) throws IOException {
				//store reference to value, it is needed to notify listeners
				V deletedValue = (V) getRecordManager().fetch(key,valueSerializer);
				
				for(RecordListener<Long,V> l:listeners)
					l.recordRemoved(key, deletedValue);
				
				//dispose old reference
				getRecordManager().delete(key);
			}

			public void recordUpdated(Long key, String oldValue, String newValue) throws IOException {				
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
		listeners.add((RecordListener<Long, V>) listener);
	}

	public RecordManager getRecordManager() {
		return map.getRecordManager();
	}

	public void removeRecordListener(RecordListener<Long, V> listener) {	
		listeners.remove(listener);
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
			try {
				V oldVal= getRecordManager().fetch(key,valueSerializer,true);
				getRecordManager().update(key, value,valueSerializer);
				//fire listeners, recid tree map did not change, so they would not be notified
				for(RecordListener<Long, V> listener: listeners)
					listener.recordUpdated(key, oldVal, value);
				return oldVal;
			} catch (IOException e) {
				throw new IOError(e);
			}
			
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
