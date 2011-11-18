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

import java.io.IOError;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

/**
 *  An interface/abstract class to manage records, which are objects serialized to byte[] on background.
 *  <p>
 *  The set of record operations is simple: fetch, insert, update and delete.
 *  Each record is identified using a "rowid" and contains a byte[] data block serialized to object.
 *  Rowids are returned on inserts and you can store them someplace safe
 *  to be able to get  back to them.  Data blocks can be as long as you wish,
 *  and may have lengths different from the original when updating.
 *  <p>
 *  RecordManager is responsible for handling transactions.
 *  JDBM2 supports only single transaction for data store.
 *  See <code>commit</code> and <code>roolback</code> methods for more details.
 *  <p>
 *  RecordManager is also factory for primary Maps.
 *  <p>
 * @author Jan Kotek
 * @author Alex Boisvert
 * @author Cees de Groot
 */
public abstract class RecordManager {



    /**
     *  Inserts a new record using standard java object serialization.
     *
     *  @param obj the object for the new record.
     *  @return the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public long insert( Object obj ) throws IOException{
        return insert(obj,defaultSerializer());
    }


    /**
     *  Inserts a new record using a custom serializer.
     *
     *  @param obj the object for the new record.
     *  @param serializer a custom serializer
     *  @return the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public abstract <A> long insert( A obj, Serializer<A> serializer ) throws IOException;


    /**
     *  Deletes a record.
     *
     *  @param recid the rowid for the record that should be deleted.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public abstract void delete( long recid ) throws IOException;


    /**
     *  Updates a record using standard java object serialization.
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param obj the new object for the record.
     *  @throws IOException when one of the underlying I/O operations fails or given recid does not exists.
     */
    public void update( long recid, Object obj ) throws IOException{
        update(recid,obj, defaultSerializer());
    }



    /**
     *  Updates a record using a custom serializer.
     *  If given recid does not exist, IOException will be thrown before/during commit (cache).
     *
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param obj the new object for the record.
     *  @param serializer a custom serializer
     *  @throws IOException when one of the underlying I/O operations fails
     */
    public abstract <A> void update( long recid, A obj, Serializer<A> serializer )
        throws IOException;


    /**
     *  Fetches a record using standard java object serialization.
     *  If given recid does not exist, IOException will be thrown before/during commit (cache).
     *
     *  @param recid the recid for the record that must be fetched.
     *  @return the object contained in the record, null if given recid does not exist
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public <A> A fetch( long recid ) throws IOException{
        return (A) fetch(recid,defaultSerializer());
    }

    /**
     *  Fetches a record using a custom serializer.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @param serializer a custom serializer
     *  @return the object contained in the record, null if given recid does not exist
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public abstract <A> A fetch( long recid, Serializer<A> serializer )
        throws IOException;

    /**
     *  Fetches a record using a custom serializer and optionaly disabled cache
     *
     *  @param recid the recid for the record that must be fetched.
     *  @param serializer a custom serializer
     *  @param disableCache true to disable any caching mechanism
     *  @return the object contained in the record, null if given recid does not exist
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public abstract <A> A fetch( long recid, Serializer<A> serializer, boolean disableCache )
        throws IOException;

    /**
     *  Closes the record manager and release resources.
     *  Record manager can not be used after it was closed
     *
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public abstract void close()
        throws IOException;


    /**
     * Empty cache. This may be usefull if you need to release memory.
     *
     * @throws IOException
     */
    public abstract void clearCache() throws IOException;

    /**
     * Defragments storage, so it consumes less space.
     * This commits any uncommited data.
     *
     * @throws IOException
     */
    public abstract void defrag() throws IOException;



    /**
     * Commit (make persistent) all changes since beginning of transaction.
     * JDBM supports only single transaction.
     */
    public abstract void commit()
        throws IOException;


    /**
     * Rollback (cancel) all changes since beginning of transaction.
     * JDBM supports only single transaction.
     * This operations affects all maps created by this RecordManager.
     */
    public abstract void rollback()
        throws IOException;




    /**
     * Creates or load existing Primary Hash Map which persists data into DB.
     *
     *
     * @param <K> Key type
     * @param <V> Value type
     * @param name record name
     * @return
     */
    public <K, V> PrimaryHashMap<K, V> createHashMap(String name) {
        return createHashMap(name, null, null);
    }


    /**
     * Creates or load existing Primary Hash Map which persists data into DB.
     * Map will use custom serializers for Keys and Values.
     * Leave keySerializer null to use default serializer for keys
     *
     * @param <K> Key type
     * @param <V> Value type
     * @param name record name
     * @param keySerializer serializer to be used for Keys, leave null to use default serializer
     * @param valueSerializer serializer to be used for Values
     * @return
     */
	public synchronized <K, V> PrimaryHashMap<K, V> createHashMap(String name, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
		try{
			HTree<K, V> tree = null;
        
			long recid = getNamedObject( name);
			if ( recid != 0 ) {
				tree = new HTree( this, recid, keySerializer, valueSerializer);
			} else {
				tree = new HTree(this, keySerializer, valueSerializer);
				setNamedObject( name, tree.getRecid() );
			}
			return tree;
		}catch(IOException  e){
			throw new IOError(e);
		}
	}

        public synchronized <K> Set<K> createHashSet(String name) {
            return createHashSet(name,null);
        }

        public synchronized <K> Set<K> createHashSet(String name, Serializer<K> keySerializer) {
                return new HTreeSet(createHashMap(name,keySerializer,null));
	}

    /**
     * Creates or load existing Primary TreeMap which persists data into DB.
     *
     *
     * @param <K> Key type
     * @param <V> Value type
     * @param name record name
     * @return
     */
	public <K extends Comparable, V> PrimaryTreeMap<K, V> createTreeMap(String name) {
		return createTreeMap(name, null,null,null);
	}


    /**
     * Creates or load existing TreeMap which persists data into DB.
     *
     * @param <K> Key type
     * @param <V> Value type
     * @param name record name
     * @param keyComparator Comparator used to sort keys
     * @param valueSerializer Serializer used for values. This may reduce disk space usage
     * @param keySerializer Serializer used for keys. This may reduce disk space usage     *
     * @return
     */
	public synchronized <K, V> PrimaryTreeMap<K, V> createTreeMap(String name,
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


        public synchronized <K> SortedSet<K> createTreeSet(String name) {
            return createTreeSet(name, null, null);
        }


        public synchronized <K> SortedSet<K> createTreeSet(String name, Comparator<K> keyComparator,Serializer<K> keySerializer) {
            return new BTreeSet<K>(createTreeMap(name,
                    keyComparator != null ? keyComparator : Utils.COMPARABLE_COMPARATOR,
                    null, keySerializer));
        }


    public <K> List<K> createLinkedList(String name){
        return createLinkedList(name, null);
    }

    public <K> List<K> createLinkedList(String name, Serializer<K> serializer){
        try{
            //TODO support value serializer at JDBMLinkedLIst
            long recid = getNamedObject( name);
            if ( recid != 0 )
                throw new IllegalArgumentException("LinkedList with name '"+name+"' already exists");

             //allocate record and overwrite it
            recid = insert(null);
            LinkedList<K> list = new LinkedList<K>(this,recid,serializer);
            update(recid,list);
            setNamedObject( name, recid );


            return list;
        }catch (IOException e){
            throw new IOError(e);
        }
    }

    public <K> List<K> loadLinkedList(String name){
        try{
            long recid = getNamedObject( name);
            if ( recid == 0 )
                throw new IllegalArgumentException("LinkedList with name '"+name+"' does not exist");

            LinkedList<K> list = (LinkedList<K>) fetch(recid);
            list.setRecmanAndListRedic(this, recid);
            return list;
        }catch (IOException e){
            throw new IOError(e);
        }
    }







    /**
     * Obtain the record id of a named object. Returns 0 if named object
     * doesn't exist.
     * Named objects are used to store Map views and other well known objects.
     */
    protected abstract long getNamedObject( String name )
        throws IOException;


    /**
     * Set the record id of a named object.
     * Named objects are used to store Map views and other well known objects.
     */
    protected abstract void setNamedObject( String name, long recid )
        throws IOException;

    protected abstract Serializer defaultSerializer();
	
}
