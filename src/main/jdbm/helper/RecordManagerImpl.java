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
		try{
			HTree<K, V> tree = null;
        
			long recid = getNamedObject( name);
			if ( recid != 0 ) {
				tree = HTree.load( this, recid );
			} else {
				tree = HTree.createInstance(this);
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

    /**
     * Creates or load existing TreeMap which persists data into DB.
     * 
     * 
     * @param <K> Key type
     * @param <V> Value type
     * @param name record name
     * @param keyComparator Comparator used to sort keys
     * @return
     */
	public <K, V> PrimaryTreeMap<K, V> treeMap(String name, Comparator<K> keyComparator) {
		return treeMap(name, keyComparator, null);
	}

	public <K, V> PrimaryTreeMap<K, V> treeMap(String name,
			Comparator<K> keyComparator, Serializer<V> valueSerializer) {
		try{
			BTree<K,V> tree = null;
        
			// create or load fruit basket (hashtable of fruits)
			long recid = getNamedObject( name);
			if ( recid != 0 ) {
				tree = BTree.load( this, recid );
			} else {
				tree = BTree.createInstance(this,keyComparator);
				setNamedObject( name, tree.getRecid() );
			}
			return tree.asMap();
		}catch(IOException  e){
			throw new IOError(e);
		}	
	}

	public <V> PrimaryStoreMap<Long, V> storeMap(String name,
				Serializer<V> valueSerializer) {
		try{
			BTree<Long,Object> tree = null;
        
			// create or load fruit basket (hashtable of fruits)
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
