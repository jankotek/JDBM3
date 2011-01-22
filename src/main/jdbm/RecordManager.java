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

import java.io.IOException;
import java.util.Comparator;

/**
 *  An interface to manages records, which are objects serialized to byte[] on background.
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
 *  RecordManader is also factory for primary Maps. 
 *  <p>
 * @author <a href="mailto:opencoeli@gmail.com">Jan Kotek</a>
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @author <a href="cg@cdegroot.com">Cees de Groot</a>
 */
public interface  RecordManager
{


    /**
     * Recid indicating no record (e.g. null)
     */
    public static final long NULL_RECID = 0;



    /**
     *  Inserts a new record using standard java object serialization.
     *
     *  @param obj the object for the new record.
     *  @return the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public abstract long insert( Object obj )
        throws IOException;

    
    /**
     *  Inserts a new record using a custom serializer.
     *
     *  @param obj the object for the new record.
     *  @param serializer a custom serializer
     *  @return the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public abstract <A> long insert( A obj, Serializer<A> serializer )
        throws IOException;


    /**
     *  Deletes a record.
     *
     *  @param recid the rowid for the record that should be deleted.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public abstract void delete( long recid )
        throws IOException;


    /**
     *  Updates a record using standard java object serialization.
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param obj the new object for the record.
     *  @throws IOException when one of the underlying I/O operations fails or given recid does not exists.
     */
    public abstract void update( long recid, Object obj )
        throws IOException;


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
    public abstract Object fetch( long recid )
        throws IOException;


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
     * Obtain the record id of a named object. Returns 0 if named object
     * doesn't exist.
     * Named objects are used to store Map views and other well known objects.
     */
    public abstract long getNamedObject( String name )
        throws IOException;


    /**
     * Set the record id of a named object.
     * Named objects are used to store Map views and other well known objects.
     */
    public abstract void setNamedObject( String name, long recid )
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
	public <K, V> PrimaryHashMap<K, V> hashMap(String name);



    /**
     * Creates or load existing Primary Hash Map which persists data into DB.
     * This method uses custom serializer for keys.
     *
     * @param <K> Key type
     * @param <V> Value type
     * @param name record name
     * @param keySerializer serializer to be used for Keys
     * @return
     */
	public <K, V> PrimaryHashMap<K, V> hashMap(String name, Serializer<K> keySerializer);



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
	public <K, V> PrimaryHashMap<K, V> hashMap(String name, Serializer<K> keySerializer, Serializer<V> valueSerializer);

    /**
     * Creates or load existing Primary TreeMap which persists data into DB.
     * 
     * 
     * @param <K> Key type
     * @param <V> Value type
     * @param name record name
     * @return
     */

	@SuppressWarnings("unchecked")
	public <K extends Comparable, V> PrimaryTreeMap<K, V> treeMap(String name);

    /**
     * Creates or load existing TreeMap which persists data into DB.
     * 
     * @param <K> Key type
     * @param <V> Value type
     * @param name record name
     * @param valueSerializer Serializer used for values. This may reduce disk space usage.
     * @return
     */
	@SuppressWarnings("unchecked")
	public <K extends Comparable, V> PrimaryTreeMap<K, V> treeMap(String name, Serializer<V> valueSerializer);
	
    /**
     * Creates or load existing TreeMap which persists data into DB.
     * 
     * @param <K> Key type
     * @param <V> Value type
     * @param name record name
     * @param valueSerializer Serializer used for values. This may reduce disk space usage.
     * @param keySerializer Serializer used for keys. This may reduce disk space usage.
     * @return
     */
	@SuppressWarnings("unchecked")
	public <K extends Comparable, V> PrimaryTreeMap<K, V> treeMap(String name, 
			Serializer<V> valueSerializer, Serializer<K> keySerializer);
	
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
	public <K, V> PrimaryTreeMap<K, V> treeMap(String name, Comparator<K> keyComparator);

    /**
     * Creates or load existing TreeMap which persists data into DB.
     * 
     * 
     * @param <K> Key type
     * @param <V> Value type
     * @param name record name
     * @param keyComparator Comparator used to sort keys
     * @param valueSerializer Serializer used for values. This may reduce disk space usage
     * @return
     */
	public <K, V> PrimaryTreeMap<K, V> treeMap(String name,
			Comparator<K> keyComparator, Serializer<V> valueSerializer) ;
	
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
	public <K, V> PrimaryTreeMap<K, V> treeMap(String name,
			Comparator<K> keyComparator, Serializer<V> valueSerializer, Serializer<K> keySerializer) ;
	
	/**
     * Creates or load existing StoreMap which persists data into DB.
     *  
     * @param <V> Value type
     * @param name record name
     * @param valueSerializer Serializer used for values. This may reduce disk space usage
     * @return map
     */
	public <V> PrimaryStoreMap<Long, V> storeMap(String name,
				Serializer<V> valueSerializer) ;
	
	/**
     * Creates or load existing Primary StoreMap which persists data into DB.
     *  
     * @param <V> Value type
     * @param name record name
     * @return map
     */
	public <V> PrimaryStoreMap<Long, V> storeMap(String name);


	

}

