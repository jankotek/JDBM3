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
import java.util.Comparator;

import jdbm.PrimaryHashMap;
import jdbm.PrimaryStoreMap;
import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.Serializer;
import jdbm.btree.BTree;
import jdbm.htree.HTree;

/**
 * Abstract class for record manager which implements most of stuff
 * 
 * @author Jan Kotek
 *
 */
public abstract class RecordManagerImpl implements RecordManager{
	
	public <K, V> PrimaryHashMap<K, V> hashMap(String name) {
        return hashMap(name,null,null);
    }

    public <K, V> PrimaryHashMap<K, V> hashMap(String name, Serializer<K> keySerializer) {
        return hashMap(name,keySerializer,null);
    }

	public synchronized <K, V> PrimaryHashMap<K, V> hashMap(String name, Serializer<K> keySerializer,  Serializer<V> valueSerializer) {
		try{
			HTree<K, V> tree = null;
        
			long recid = getNamedObject( name);
			if ( recid != 0 ) {
				tree = HTree.load( this, recid, keySerializer, valueSerializer);
			} else {
				tree = HTree.createInstance(this, keySerializer, valueSerializer);
				setNamedObject( name, tree.getRecid() );
			}
			return tree.asMap();
		}catch(IOException  e){
			throw new IOError(e);
		}
	}

	@SuppressWarnings("unchecked")
	public <K extends Comparable, V> PrimaryTreeMap<K, V> treeMap(String name) {
		return treeMap(name, ComparableComparator.INSTANCE);
	}


	@SuppressWarnings("unchecked")
	public <K extends Comparable, V> PrimaryTreeMap<K, V> treeMap(String name, Serializer<V> valueSerializer) {
		return treeMap(name, ComparableComparator.INSTANCE, valueSerializer);
	}
	
	@SuppressWarnings("unchecked")
	public <K extends Comparable, V> PrimaryTreeMap<K, V> treeMap(String name, Serializer<V> valueSerializer, Serializer<K> keySerializer) {	
		return treeMap(name, ComparableComparator.INSTANCE, valueSerializer,keySerializer);
	}


	public <K, V> PrimaryTreeMap<K, V> treeMap(String name, Comparator<K> keyComparator) {
		return treeMap(name, keyComparator, null);
	}
	


	public <K, V> PrimaryTreeMap<K, V> treeMap(String name,
		Comparator<K> keyComparator, Serializer<V> valueSerializer) {
		return treeMap(name,keyComparator,valueSerializer,null);
	}

	public synchronized <K, V> PrimaryTreeMap<K, V> treeMap(String name,
			Comparator<K> keyComparator, Serializer<V> valueSerializer, Serializer<K> keySerializer) {
		try{
			BTree<K,V> tree = null;
        
			// create or load 
			long recid = getNamedObject( name);
			if ( recid != 0 ) {
				tree = BTree.load( this, recid );
			} else {
				tree = BTree.createInstance(this,keyComparator);
				setNamedObject( name, tree.getRecid() );
			}
			tree.setKeySerializer(keySerializer);
			tree.setValueSerializer(valueSerializer);
			
			return tree.asMap();
		}catch(IOException  e){
			throw new IOError(e);
		}	
	}

	public synchronized <V> PrimaryStoreMap<Long, V> storeMap(String name,
				Serializer<V> valueSerializer) {
		try{
			BTree<Long,String> tree = null;
        
			// create or load
			long recid = getNamedObject( name);
			if ( recid != 0 ) {
				tree = BTree.load( this, recid );
			} else {
				tree = BTree.createInstance(this);
				setNamedObject( name, tree.getRecid() );
			}
			return new PrimaryStoreMapImpl<Long, V>(tree.asMap(),valueSerializer);
		}catch(IOException  e){
			throw new IOError(e);
		}	
	}
	

	@SuppressWarnings("unchecked")
	public <V> PrimaryStoreMap<Long, V> storeMap(String name){
		return storeMap(name,(Serializer<V>)DefaultSerializer.INSTANCE);
	}



    public void update( long recid, Object obj ) throws IOException{
    	update( recid, obj, DefaultSerializer.INSTANCE );
    }
    
    public Object fetch( long recid ) throws IOException{
    	return fetch( recid, DefaultSerializer.INSTANCE );
    }

    public long insert( Object obj )throws IOException{
    	return insert( obj, DefaultSerializer.INSTANCE );
    }


	
}
