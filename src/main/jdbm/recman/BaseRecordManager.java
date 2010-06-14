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
 * $Id: BaseRecordManager.java,v 1.8 2005/06/25 23:12:32 doomdark Exp $
 */

package jdbm.recman;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import jdbm.RecordManager;
import jdbm.Serializer;
import jdbm.helper.DefaultSerializer;
import jdbm.helper.OpenByteArrayInputStream;
import jdbm.helper.OpenByteArrayOutputStream;

/**
 *  This class manages records, which are uninterpreted blobs of data. The
 *  set of operations is simple and straightforward: you communicate with
 *  the class using long "rowids" and byte[] data blocks. Rowids are returned
 *  on inserts and you can stash them away someplace safe to be able to get
 *  back to them. Data blocks can be as long as you wish, and may have
 *  lengths different from the original when updating.
 *  <p>
 *  Operations are synchronized, so that only one of them will happen
 *  concurrently even if you hammer away from multiple threads. Operations
 *  are made atomic by keeping a transaction log which is recovered after
 *  a crash, so the operations specified by this interface all have ACID
 *  properties.
 *  <p>
 *  You identify a file by just the name. The package attaches <tt>.db</tt>
 *  for the database file, and <tt>.lg</tt> for the transaction log. The
 *  transaction log is synchronized regularly and then restarted, so don't
 *  worry if you see the size going up and down.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @author <a href="cg@cdegroot.com">Cees de Groot</a>
 * @version $Id: BaseRecordManager.java,v 1.8 2005/06/25 23:12:32 doomdark Exp $
 */
public final class BaseRecordManager
    extends RecordManager
{

    /**
     * Underlying record file.
     */
    private RecordFile _file;


    /**
     * Physical row identifier manager.
     */
    private PhysicalRowIdManager _physMgr;


    /**
     * Logigal to Physical row identifier manager.
     */
    private LogicalRowIdManager _logMgr;


    /**
     * Page manager.
     */
    private PageManager _pageman;


    /**
     * Reserved slot for name directory.
     */
    public static final int NAME_DIRECTORY_ROOT = 0;


    /**
     * Static debugging flag
     */
    public static final boolean DEBUG = false;

    
    /**
     * Directory of named JDBMHashtables.  This directory is a persistent
     * directory, stored as a Hashtable.  It can be retrived by using
     * the NAME_DIRECTORY_ROOT.
     */
    private Map<String,Long> _nameDirectory;

    /** is Inflate compression on */
	private boolean compress = false;

	/** deflater used to compress data if needed*/
	private Deflater deflater;
	/** inflater used to decompress data if needed*/
	private Inflater inflater;
	
	private static final int BUFFER_SIZE = RecordFile.BLOCK_SIZE * 4;
	
	private final byte[] _insertBuffer = new byte[BUFFER_SIZE];
	private final OpenByteArrayOutputStream _insertBAO = new OpenByteArrayOutputStream(_insertBuffer);
	private final DataOutputStream _insertOut = new DataOutputStream(_insertBAO);
	private final OpenByteArrayInputStream _insertBAI = new OpenByteArrayInputStream(_insertBuffer);
	private final DataInputStream _insertIn = new DataInputStream(_insertBAI);
	volatile private boolean bufferInUse = false; 


    /**
     *  Creates a record manager for the indicated file
     *
     *  @throws IOException when the file cannot be opened or is not
     *          a valid file content-wise.
     */
    public BaseRecordManager( String filename )
        throws IOException
    {
        _file = new RecordFile( filename );
        _pageman = new PageManager( _file );
        _physMgr = new PhysicalRowIdManager( _file, _pageman );
        _logMgr = new LogicalRowIdManager( _file, _pageman );
    }


    /**
     *  Get the underlying Transaction Manager
     */
    public synchronized TransactionManager getTransactionManager()
    {
        checkIfClosed();

        return _file.txnMgr;
    }


    /**
     *  Switches off transactioning for the record manager. This means
     *  that a) a transaction log is not kept, and b) writes aren't
     *  synch'ed after every update. This is useful when batch inserting
     *  into a new database.
     *  <p>
     *  Only call this method directly after opening the file, otherwise
     *  the results will be undefined.
     */
    public synchronized void disableTransactions()
    {
        checkIfClosed();

        _file.disableTransactions();
    }
    
    /**
     * Enable or disable compression of blocks with Deflate alghoritm
     * @param b
     */
	public synchronized void setCompress(boolean b) {
		checkIfClosed();
		if(DEBUG)
			System.out.println("Setting compression to: "+b);
		compress = b;		
	}


    
    /**
     *  Closes the record manager.
     *
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void close()
        throws IOException
    {
        checkIfClosed();

        _pageman.close();
        _pageman = null;

        _file.close();
        _file = null;
    }


    /**
     *  Inserts a new record using standard java object serialization.
     *
     *  @param obj the object for the new record.
     *  @return the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public long insert( Object obj )
        throws IOException
    {
        return insert( obj, DefaultSerializer.INSTANCE );
    }

    
    /**
     *  Inserts a new record using a custom serializer.
     *
     *  @param obj the object for the new record.
     *  @param serializer a custom serializer
     *  @return the rowid for the new record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized <A> long insert( A obj, Serializer<A> serializer )
        throws IOException
    {
                
        checkIfClosed();
    	if(bufferInUse){
    		//current reusable buffer is in use, have to fallback into creating new instances
    		byte[] buffer = new byte[1024];
    		OpenByteArrayOutputStream bao = new OpenByteArrayOutputStream(buffer);
    		DataOutputStream out = new DataOutputStream(bao);
    		return insert2(obj,serializer,buffer,bao,out);
    	}

        try{
        		
        	bufferInUse = true;
        	return insert2(obj, serializer,_insertBuffer,_insertBAO,_insertOut);
        }finally{
        	bufferInUse = false;
        }
    }


	private <A> long insert2(A obj, Serializer<A> serializer, byte[] insertBuffer, OpenByteArrayOutputStream insertBAO, DataOutputStream insertOut)
			throws IOException {
		insertBAO.reset(insertBuffer);
      
		serializer.serialize(insertOut, obj);
		if(compress){
			byte[] data = compress(insertBAO.getBuf(),insertBAO.size());
			insertBAO.reset(insertBuffer);
			insertBAO.write(data);
		}
		Location physRowId = _physMgr.insert( insertBAO.getBuf(), 0, insertBAO.size() );
		long recid = _logMgr.insert( physRowId ).toLong();
		if ( DEBUG ) {
			System.out.println( "BaseRecordManager.insert() recid " + recid + " length " + insertBAO.size() ) ;
		}
		return recid;
	}

    private synchronized byte[] compress(byte[] data, int length) throws IOException {
    	if(!compress )
    		return data;
    	if(deflater == null){
    		deflater = new Deflater();    		
    	}else{
    		deflater.reset();
    	}
    	ByteArrayOutputStream b = new ByteArrayOutputStream(0);
    	OutputStream d = new DeflaterOutputStream(b,deflater);
    	
    	d.write(data,0,length);
    	d.close();
		return b.toByteArray();
	}

    private synchronized  DataInputStream decompress(DataInputStream data) throws IOException {
    	if(!compress)
    		return data;
    	
    	if(inflater == null){
    		inflater = new Inflater();       	
    	}else{
    		inflater.reset();
    	}
    	
    	return new DataInputStream(new InflaterInputStream(data,inflater));    	
	}

	/**
     *  Deletes a record.
     *
     *  @param recid the rowid for the record that should be deleted.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized void delete( long recid )
        throws IOException
    {
        checkIfClosed();
        if ( recid <= 0 ) {
            throw new IllegalArgumentException( "Argument 'recid' is invalid: "
                                                + recid );
        }

        if ( DEBUG ) {
            System.out.println( "BaseRecordManager.delete() recid " + recid ) ;
        }

        Location logRowId = new Location( recid );
        Location physRowId = _logMgr.fetch( logRowId );
        _physMgr.delete( physRowId );
        _logMgr.delete( logRowId );
    }


    /**
     *  Updates a record using standard java object serialization.
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param obj the new object for the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public void update( long recid, Object obj )
        throws IOException
    {
        update( recid, obj, DefaultSerializer.INSTANCE );
    }

    
    /**
     *  Updates a record using a custom serializer.
     *
     *  @param recid the recid for the record that is to be updated.
     *  @param obj the new object for the record.
     *  @param serializer a custom serializer
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized  <A> void update( long recid, A obj, Serializer<A> serializer )
        throws IOException
    {
        checkIfClosed();
        if ( recid <= 0 ) {
            throw new IllegalArgumentException( "Argument 'recid' is invalid: "
                                                + recid );
        }
    	if(bufferInUse){
    		//current reusable buffer is in use, have to fallback into creating new instances
    		byte[] buffer = new byte[1024];
    		OpenByteArrayOutputStream bao = new OpenByteArrayOutputStream(buffer);
    		DataOutputStream out = new DataOutputStream(bao);
    		update2(recid,obj,serializer,buffer,bao,out);
    		return;
    	}

        try{        
        	bufferInUse = true;
        	update2(recid, obj, serializer,_insertBuffer, _insertBAO, _insertOut);
        }finally{
        	bufferInUse = false;
        }
    }


	private <A> void update2(long recid, A obj, Serializer<A> serializer,byte[] insertBuffer, OpenByteArrayOutputStream insertBAO, DataOutputStream insertOut)
			throws IOException {
		Location logRecid = new Location( recid );
		Location physRecid = _logMgr.fetch( logRecid );
		insertBAO.reset(insertBuffer);
		serializer.serialize(insertOut, obj );

		if(compress){
			byte[] data = compress(insertBAO.getBuf(),insertBAO.size());
			insertBAO.reset(insertBuffer);
			insertBAO.write(data);
		}
		
		if ( DEBUG ) {
			System.out.println( "BaseRecordManager.update() recid " + recid + " length " + insertBAO.size() ) ;
		}
      
		Location newRecid = _physMgr.update( physRecid, insertBAO.getBuf(), 0, insertBAO.size() );
		if ( ! newRecid.equals( physRecid ) ) {
			_logMgr.update( logRecid, newRecid );
		}
	}


    /**
     *  Fetches a record using standard java object serialization.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @return the object contained in the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public Object fetch( long recid )
        throws IOException
    {
        return fetch( recid, DefaultSerializer.INSTANCE );
    }


    /**
     *  Fetches a record using a custom serializer.
     *
     *  @param recid the recid for the record that must be fetched.
     *  @param serializer a custom serializer
     *  @return the object contained in the record.
     *  @throws IOException when one of the underlying I/O operations fails.
     */
    public synchronized <A> A fetch( long recid, Serializer<A> serializer )
        throws IOException
    {        

        checkIfClosed();
        if ( recid <= 0 ) {
            throw new IllegalArgumentException( "Argument 'recid' is invalid: "
                                                + recid );
        }
        
    	if(bufferInUse){
    		//current reusable buffer is in use, have to fallback into creating new instances
    		byte[] buffer = new byte[1024];
    		OpenByteArrayOutputStream bao = new OpenByteArrayOutputStream(buffer);
    		DataOutputStream out = new DataOutputStream(bao);
    		OpenByteArrayInputStream bai = new OpenByteArrayInputStream(buffer);
    		DataInputStream in = new DataInputStream(bai);
    		return fetch2(recid,serializer,buffer,bao,out, bai,in);
    	}
        try{
        	bufferInUse = true;

        	return fetch2(recid, serializer,_insertBuffer,_insertBAO,_insertOut,_insertBAI, _insertIn);
        }finally{
        	bufferInUse = false;
        }
    }


	private <A> A fetch2(long recid, Serializer<A> serializer,byte[] insertBuffer, 
				OpenByteArrayOutputStream insertBAO, DataOutputStream insertOut,
				OpenByteArrayInputStream insertBAI, DataInputStream insertIn)
			throws IOException {
		insertBAO.reset(insertBuffer);
		_physMgr.fetch(insertBAO, _logMgr.fetch( new Location( recid ) ) );

		if ( DEBUG ) {
			System.out.println( "BaseRecordManager.fetch() recid " + recid + " length " + insertBAO.size() ) ;
		}
		insertBAI.reset(insertBAO.getBuf(), insertBAO.size());
		try{
			if(compress)
				return serializer.deserialize(decompress( insertIn ));
			else
				return serializer.deserialize( insertIn );
		}catch(ClassNotFoundException e){
			throw new IOError(e);
		}
	}


    /**
     *  Returns the number of slots available for "root" rowids. These slots
     *  can be used to store special rowids, like rowids that point to
     *  other rowids. Root rowids are useful for bootstrapping access to
     *  a set of data.
     */
    public int getRootCount()
    {
        return FileHeader.NROOTS;
    }

    /**
     *  Returns the indicated root rowid.
     *
     *  @see #getRootCount
     */
    public synchronized long getRoot( int id )
        throws IOException
    {
        checkIfClosed();

        return _pageman.getFileHeader().getRoot( id );
    }


    /**
     *  Sets the indicated root rowid.
     *
     *  @see #getRootCount
     */
    public synchronized void setRoot( int id, long rowid )
        throws IOException
    {
        checkIfClosed();

        _pageman.getFileHeader().setRoot( id, rowid );
    }


    /**
     * Obtain the record id of a named object. Returns 0 if named object
     * doesn't exist.
     */
    public long getNamedObject( String name )
        throws IOException
    {
        checkIfClosed();

        Map<String,Long> nameDirectory = getNameDirectory();
        Long recid = (Long) nameDirectory.get( name );
        if ( recid == null ) {
            return 0;
        }
        return recid.longValue();
    }

    /**
     * Set the record id of a named object.
     */
    public void setNamedObject( String name, long recid )
        throws IOException
    {
        checkIfClosed();

        Map<String,Long> nameDirectory = getNameDirectory();
        if ( recid == 0 ) {
            // remove from hashtable
            nameDirectory.remove( name );
        } else {
            nameDirectory.put( name, new Long( recid ) );
        }
        saveNameDirectory( nameDirectory );
    }


    /**
     * Commit (make persistent) all changes since beginning of transaction.
     */
    public synchronized void commit()
        throws IOException
    {
        checkIfClosed();

        _pageman.commit();
    }


    /**
     * Rollback (cancel) all changes since beginning of transaction.
     */
    public synchronized void rollback()
        throws IOException
    {
        checkIfClosed();

        _pageman.rollback();
    }


    /**
     * Load name directory
     */
    @SuppressWarnings("unchecked")
	private Map<String,Long> getNameDirectory()
        throws IOException
    {
        // retrieve directory of named hashtable
        long nameDirectory_recid = getRoot( NAME_DIRECTORY_ROOT );
        if ( nameDirectory_recid == 0 ) {
            _nameDirectory = new HashMap<String,Long>();
            nameDirectory_recid = insert( _nameDirectory );
            setRoot( NAME_DIRECTORY_ROOT, nameDirectory_recid );
        } else {
            _nameDirectory = (Map<String,Long>) fetch( nameDirectory_recid );
        }
        return _nameDirectory;
    }


    private void saveNameDirectory( Map<String,Long> directory )
        throws IOException
    {
        long recid = getRoot( NAME_DIRECTORY_ROOT );
        if ( recid == 0 ) {
            throw new IOException( "Name directory must exist" );
        }
        update( recid, _nameDirectory );
    }


    /**
     * Check if RecordManager has been closed.  If so, throw an
     * IllegalStateException.
     */
    private void checkIfClosed()
        throws IllegalStateException
    {
        if ( _file == null ) {
            throw new IllegalStateException( "RecordManager has been closed" );
        }
    }


}
