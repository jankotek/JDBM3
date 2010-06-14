/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
 * Copyright 2000-2001 (C) Alex Boisvert. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 * $Id: RecordManager.java,v 1.3 2005/06/25 23:12:31 doomdark Exp $
 */

package jdbm;

import java.io.IOError;
import java.io.IOException;
import java.util.Comparator;

import jdbm.btree.BTree;
import jdbm.helper.ComparableComparator;
import jdbm.helper.DefaultSerializer;
import jdbm.helper.PrimaryStoreMapImpl;
import jdbm.helper.StoreReference;
import jdbm.htree.HTree;

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
public abstract class RecordManager
{

    /**
     * Reserved slot for name directory.
     */
    public static final int NAME_DIRECTORY_ROOT = 0;
	

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
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public abstract void update( long recid, Object obj )
        throws IOException;


    /**
     *  Updates a record using a custom serializer.
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param obj the new object for the record.
     *  @param serializer a custom serializer
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public abstract <A> void update( long recid, A obj, Serializer<A> serializer )
        throws IOException;

    
    /**
     *  Fetches a record using standard java object serialization.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @return the object contained in the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public abstract Object fetch( long recid )
        throws IOException;


    /**
     *  Fetches a record using a custom serializer.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @param serializer a custom serializer
     *  @return the object contained in the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public abstract <A> A fetch( long recid, Serializer<A> serializer )
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
     *  Returns the number of slots available for "root" rowids. These slots
     *  can be used to store special rowids, like rowids that point to
     *  other rowids. Root rowids are useful for bootstrapping access to
     *  a set of data.
     */
    public abstract int getRootCount();


    /**
     *  Returns the indicated root rowid.
     *
     *  @see #getRootCount
     */
    public abstract long getRoot( int id )
        throws IOException;


    /**
     *  Sets the indicated root rowid.
     *
     *  @see #getRootCount
     */
    public abstract void setRoot( int id, long rowid )
        throws IOException;


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
	public <K extends Comparable, V> PrimaryTreeMap<K, V> treeMap(String name) {
		return treeMap(name, ComparableComparator.INSTANCE);
	}

    /**
     * Creates or load existing TreeMap which persists data into DB.
     * 
     * 
     * @param <K> Key type
     * @param <V> Value type
     * @param name record name
     * @param valueSerializer Serializer used for values. This may reduce disk space usage.
     * @return
     */
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

	/**
     * Creates or load existing StoreMap which persists data into DB.
     *  
     * @param <V> Value type
     * @param name record name
     * @param valueSerializer Serializer used for values. This may reduce disk space usage
     * @return map
     */
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
	
	/**
     * Creates or load existing Primary StoreMap which persists data into DB.
     *  
     * @param <V> Value type
     * @param name record name
     * @return map
     */
	@SuppressWarnings("unchecked")
	public <V> PrimaryStoreMap<Long, V> storeMap(String name){
		return storeMap(name,(Serializer<V>)DefaultSerializer.INSTANCE);
	}


}

