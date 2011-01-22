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
package jdbm.htree;

import java.io.IOError;
import java.io.IOException;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import jdbm.PrimaryHashMap;
import jdbm.RecordListener;
import jdbm.RecordManager;
import jdbm.helper.AbstractPrimaryMap;


public class HTreeMap<K,V> extends AbstractPrimaryMap<K,V> implements PrimaryHashMap<K,V>{
	
	protected final HTree<K,V> tree;
	protected final  boolean readonly;
	
	public HTreeMap(HTree<K,V> tree, boolean readonly){
		this.tree = tree;
		this.readonly = readonly;
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return new AbstractSet<Entry<K,V>>(){
			
			protected Entry<K,V> newEntry(K k,V v){
				return new SimpleEntry<K,V>(k,v){
					private static final long serialVersionUID = 978651696969194154L;

					public V setValue(V arg0) {
						HTreeMap.this.put(getKey(), arg0);
						return super.setValue(arg0);
					};
					
				};
			}

			public boolean add(java.util.Map.Entry<K, V> e) {
				if(readonly)
					throw new UnsupportedOperationException("readonly");

				try {
					if(e.getKey() == null)
						throw new NullPointerException("Can not add null key");
					if(e.getValue().equals(tree.find(e.getKey())))
							return false;
					tree.put(e.getKey(), e.getValue());
					return true;
				} catch (IOException e1) {
					throw new IOError(e1);
				}
			}

			@SuppressWarnings("unchecked")
			public boolean contains(Object o) {
				if(o instanceof Entry){
					Entry<K,V> e = (java.util.Map.Entry<K, V>) o;
					try {
						if(e.getKey()!=null && tree.find(e.getKey())!=null)
							return true;
					} catch (IOException e1) {
						throw new IOError(e1);
					}
				}
				return false;
			}


			public Iterator<java.util.Map.Entry<K, V>> iterator() {
				try {
					final Iterator<K> br = tree.keys();				
				return new Iterator<Entry<K,V>>(){
					
					private Entry<K,V> next;
					private K lastKey;
					void ensureNext(){
						try{
							if(br.hasNext()){
								K k = br.next();
								next = newEntry(k,tree.find(k));
							}else
								next = null;
						}catch (IOException e){
							throw new IOError(e);
						}
					}
					{
						ensureNext();
					}
					
					

					public boolean hasNext() {
						return next!=null;
					}

					public java.util.Map.Entry<K, V> next() {
						if(next == null)
							throw new NoSuchElementException();
						Entry<K,V> ret = next;
						lastKey = ret.getKey();
						//move to next position
						ensureNext();
						return ret;
					}

					public void remove() {
						if(readonly)
							throw new UnsupportedOperationException("readonly");
						if(lastKey == null)
							throw new IllegalStateException();

							HTreeMap.this.remove(lastKey);
							lastKey = null;
					}};
					
				} catch (IOException e) {
					throw new IOError(e);
				}
					
			}

			@SuppressWarnings("unchecked")
			public boolean remove(Object o) {
				if(readonly)
					throw new UnsupportedOperationException("readonly");
				
				if(o instanceof Entry){
					Entry<K,V> e = (java.util.Map.Entry<K, V>) o;
					try {
						//check for nulls
						if(e.getKey() == null || e.getValue() == null)
							return false;
						//find old value, must be same as item in entry
						V v = get(e.getKey());
						if(v == null || !e.getValue().equals(v))
							return false;
						tree.remove(e.getKey());
						return true;
					} catch (IOException e1) {
						throw new IOError(e1);
					}
				}
				return false;

			}

			@Override
			public int size() {
				try{
					int counter = 0;
					Iterator<K> it = tree.keys();
					while(it.hasNext()){
						it.next();
						counter ++;
					}
					return counter;
				}catch (IOException e){
					throw new IOError(e);
				}
					
			}

		};
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		try{
			if(key == null)
				return null;
			return tree.find((K) key);
		}catch (ClassCastException e){
			return null;
		}catch (IOException e){
			throw new IOError(e);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public V remove(Object key) {
		if(readonly)
			throw new UnsupportedOperationException("readonly");
		
		try{
			if(key == null)
				return null;
			V oldVal = tree.find((K) key);
			if(oldVal!=null)
				tree.remove((K) key);
			return oldVal;
		}catch (ClassCastException e){
			return null;
		}catch (IOException e){
			throw new IOError(e);
		}
	}
	
	public V put(K key, V value) {
		if(readonly)
			throw new UnsupportedOperationException("readonly");

		try {
			if(key == null || value == null)
				throw new NullPointerException("Null key or value");
			V oldVal = tree.find(key);			
			tree.put(key, value);
			return oldVal;
		} catch (IOException e) {
			throw new IOError(e);
		}
	};
	
	@SuppressWarnings("unchecked")
	@Override
	public boolean containsKey(Object key) {
		if(key == null)
			return false;
		try {
			V v = tree.find((K) key);
			return v!=null;
		} catch (IOException e) {
			throw new IOError(e);
		} catch (ClassCastException e){
			return false;
		}
	}

	public HTree<K, V> getTree() {
		return tree;
	}

	
	public void addRecordListener(RecordListener<K, V> listener) {
		tree.addRecordListener(listener);
	}

	public RecordManager getRecordManager() {
		return tree.getRecordManager();
	}

	public void removeRecordListener(RecordListener<K, V> listener) {
		tree.removeRecordListener(listener);		
	}


    public void clear(){
        try{
            Iterator<K> keyIter = tree.keys();
            while(keyIter.hasNext())
                tree.remove(keyIter.next());
        }catch(IOException e){
            throw new IOError(e);
        }
    }


}
