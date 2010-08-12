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

package jdbm.recman;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import jdbm.Serializer;
import jdbm.SerializerInput;
import jdbm.SerializerOutput;
import jdbm.helper.OpenByteArrayInputStream;
import jdbm.helper.OpenByteArrayOutputStream;
import jdbm.helper.RecordManagerImpl;

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
    extends RecordManagerImpl
{

	private static final String IDR = ".idr";
	private static final String IDF = ".idf";
	private static final String DBR = ".dbr";
	private static final String DBF = ".dbf";
	static final int DATA_BLOCK_SIZE = 1024 * 8 ;
	static final int TRANS_BLOCK_SIZE = 1024 * 2;
	static final int FREE_BLOCK_SIZE = 1024;
	
	/**
	 * Version of storage. It should be safe to open lower versions, but engine should throw exception
	 * while opening new versions (as it contains unsupported features or serialization)
	 */
	static final long STORE_FORMAT_VERSION = 1L;
	
    /**
     * Underlying file for store records.
     */
    private RecordFile _physFile;

    /**
     * Page manager for physical manager.
     */
    private PageManager _physPageman;
    
    /**
     * Physical row identifier manager.
     */
    private PhysicalRowIdManager _physMgr;


    /**
     * Underlying file for store records.
     * Traces free records
     */
    private RecordFile _physFileFree;

    /**
     * Page manager for physical manager.
     * Traces free records
     */
    private PageManager _physPagemanFree;

    /**
     * Underlying file for logical records.
     */
    private RecordFile _logicFile;

    /**
     * Page manager for logical manager.
     */
    private PageManager _logicPageman;


    /**
     * Logigal to Physical row identifier manager.
     */
    private LogicalRowIdManager _logicMgr;

    /**
     * Underlying file for logical records.
     * Traces free records
     */
    private RecordFile _logicFileFree;



    /**
     * Page manager for logical manager.
     * Traces free records
     */
    private PageManager _logicPagemanFree;




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

    /**
     * Reserved slot for name directory.
     */
    public static final int NAME_DIRECTORY_ROOT = 0;
    
    
    /**
     * Reserved slot for version number
     */
    public static final int STORE_VERSION_NUMBER_ROOT = 1;
	
	

    
    /** is Inflate compression on */
	private boolean compress = false;

	/** deflater used to compress data if needed*/
	private Deflater deflater;
	/** inflater used to decompress data if needed*/
	private Inflater inflater;
	
	private static final int BUFFER_SIZE = 4096 * 2;
	
	private final byte[] _insertBuffer = new byte[BUFFER_SIZE];
	private final OpenByteArrayOutputStream _insertBAO = new OpenByteArrayOutputStream(_insertBuffer);
	private final SerializerOutput _insertOut = new SerializerOutput(_insertBAO);
	private final OpenByteArrayInputStream _insertBAI = new OpenByteArrayInputStream(_insertBuffer);
	private final SerializerInput _insertIn = new SerializerInput(_insertBAI);
	
	volatile private boolean bufferInUse = false;


	private final String _filename; 


    /**
     *  Creates a record manager for the indicated file
     *
     *  @throws IOException when the file cannot be opened or is not
     *          a valid file content-wise.
     */
    public BaseRecordManager( String filename )
        throws IOException
    {
    	_filename = filename;
    	reopen();
    }


	private void reopen() throws IOException {
		_physFileFree = new RecordFile( _filename +  DBF, FREE_BLOCK_SIZE);
    	_physPagemanFree = new PageManager(_physFileFree);    	
        _physFile = new RecordFile( _filename + DBR, DATA_BLOCK_SIZE);
        _physPageman = new PageManager( _physFile );
        _physMgr = new PhysicalRowIdManager( _physFile, _physPageman, 
        		new FreePhysicalRowIdPageManager(_physFileFree, _physPagemanFree));
                
        _logicFileFree= new RecordFile( _filename +IDF,FREE_BLOCK_SIZE );
        _logicPagemanFree = new PageManager( _logicFileFree );
        if(TRANS_BLOCK_SIZE>256*8)
        	throw new InternalError(); //to big page, slot number would not fit into page
        _logicFile = new RecordFile( _filename +IDR,TRANS_BLOCK_SIZE );
        _logicPageman = new PageManager( _logicFile );
        _logicMgr = new LogicalRowIdManager( _logicFile, _logicPageman, 
        		new FreeLogicalRowIdPageManager(_logicFileFree, _logicPagemanFree));

        long versionNumber = getRoot(STORE_VERSION_NUMBER_ROOT);
        if(versionNumber>STORE_FORMAT_VERSION)
        	throw new IOException("Unsupported version of store. Please update JDBM. Minimal supported ver:"+STORE_FORMAT_VERSION+", store ver:"+versionNumber);
        setRoot(STORE_VERSION_NUMBER_ROOT, STORE_FORMAT_VERSION);
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

        _physFile.disableTransactions();
        _logicFile.disableTransactions();
        _physFileFree.disableTransactions();
        _logicFileFree.disableTransactions();

    }
    
    /**
     * Enable or disable compression of blocks with Deflate algorithm
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

        _physPageman.close();
        _physPageman = null;

        _physFile.close();
        _physFile = null;
        
        _logicPageman.close();
        _logicPageman = null;
        
        _logicFile.close();
        _logicFile = null;
        
        _physPagemanFree.close();
        _physPagemanFree = null;

        _physFileFree.close();
        _physFileFree = null;
        
        _logicPagemanFree.close();
        _logicPagemanFree = null;
        
        _logicFileFree.close();
        _logicFileFree = null;

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
    		SerializerOutput out = new SerializerOutput(bao);
    		return insert2(obj,serializer,buffer,bao,out);
    	}

        try{
        		
        	bufferInUse = true;
        	return insert2(obj, serializer,_insertBuffer,_insertBAO,_insertOut);
        }finally{
        	bufferInUse = false;
        }
    }


	private <A> long insert2(A obj, Serializer<A> serializer, byte[] insertBuffer, OpenByteArrayOutputStream insertBAO, SerializerOutput insertOut)
			throws IOException {
		insertBAO.reset(insertBuffer);
      
		serializer.serialize(insertOut, obj);
		if(compress){
			byte[] data = compress(insertBAO.getBuf(),insertBAO.size());
			insertBAO.reset(insertBuffer);
			insertBAO.write(data);
		}
		long physRowId = _physMgr.insert( insertBAO.getBuf(), 0, insertBAO.size() );
		long recid = _logicMgr.insert( physRowId );
		if ( DEBUG ) {
			System.out.println( "BaseRecordManager.insert() recid " + recid + " length " + insertBAO.size() ) ;
		}
		return compressRecid(recid);
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

    private synchronized  SerializerInput decompress(SerializerInput data) throws IOException {
    	if(!compress)
    		return data;
    	
    	if(inflater == null){
    		inflater = new Inflater();       	
    	}else{
    		inflater.reset();
    	}
    	
    	return new SerializerInput(new InflaterInputStream(data,inflater));    	
	}

    public synchronized void delete( long logRowId )
        throws IOException
    {
    	
        checkIfClosed();
        if ( logRowId <= 0 ) {
            throw new IllegalArgumentException( "Argument 'recid' is invalid: "
                                                + logRowId );
        }

        if ( DEBUG ) {
            System.out.println( "BaseRecordManager.delete() recid " + logRowId ) ;
        }

        logRowId = decompressRecid(logRowId);
        
        long physRowId = _logicMgr.fetch( logRowId );
        _physMgr.delete( physRowId );
        _logicMgr.delete( logRowId );
    }



    public synchronized  <A> void update( long recid, A obj, Serializer<A> serializer )
        throws IOException
    {
        checkIfClosed();
        if ( recid <= 0 ) {
            throw new IllegalArgumentException( "Argument 'recid' is invalid: "
                                                + recid );
        }
    	if(bufferInUse){
    		//current reusable buffer is in use, have to create new instances
    		byte[] buffer = new byte[1024];
    		OpenByteArrayOutputStream bao = new OpenByteArrayOutputStream(buffer);
    		SerializerOutput out = new SerializerOutput(bao);
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


	private <A> void update2(long logRecid, A obj, Serializer<A> serializer,byte[] insertBuffer, OpenByteArrayOutputStream insertBAO, SerializerOutput insertOut)
			throws IOException {
		logRecid = decompressRecid(logRecid);
		long physRecid = _logicMgr.fetch( logRecid );
		if(physRecid == 0)
			throw new IOException("Can not update, recid does not exist: "+logRecid);
		insertBAO.reset(insertBuffer);
		serializer.serialize(insertOut, obj );

		if(compress){
			byte[] data = compress(insertBAO.getBuf(),insertBAO.size());
			insertBAO.reset(insertBuffer);
			insertBAO.write(data);
		}
		
		if ( DEBUG ) {
			System.out.println( "BaseRecordManager.update() recid " + logRecid + " length " + insertBAO.size() ) ;
		}
      
		long newRecid = _physMgr.update( physRecid, insertBAO.getBuf(), 0, insertBAO.size() );
		
		_logicMgr.update( logRecid, newRecid );
		
	}




    public synchronized <A> A fetch( long recid, Serializer<A> serializer )
        throws IOException
    {        

        checkIfClosed();
        if ( recid <= 0 ) {
            throw new IllegalArgumentException( "Argument 'recid' is invalid: "
                                                + recid );
        }
        
    	if(bufferInUse){
    		//current reusable buffer is in use, have to create new instances
    		byte[] buffer = new byte[1024];
    		OpenByteArrayOutputStream bao = new OpenByteArrayOutputStream(buffer);
    		SerializerOutput out = new SerializerOutput(bao);
    		OpenByteArrayInputStream bai = new OpenByteArrayInputStream(buffer);
    		SerializerInput in = new SerializerInput(bai);
    		return fetch2(recid,serializer,buffer,bao,out, bai,in);
    	}
        try{
        	bufferInUse = true;

        	return fetch2(recid, serializer,_insertBuffer,_insertBAO,_insertOut,_insertBAI, _insertIn);
        }finally{
        	bufferInUse = false;
        }
    }
    
    public synchronized <A> A fetch( long recid, Serializer<A> serializer, boolean disableCache ) throws IOException{
    	//we dont have any cache, so can ignore disableCache parameter
    	return fetch(recid, serializer);
    }



	private <A> A fetch2(long recid, Serializer<A> serializer,byte[] insertBuffer, 
				OpenByteArrayOutputStream insertBAO, SerializerOutput insertOut,
				OpenByteArrayInputStream insertBAI, SerializerInput insertIn)
			throws IOException {
		
		recid = decompressRecid(recid);
		
		insertBAO.reset(insertBuffer);		
		long physLocation = _logicMgr.fetch(recid);
		if(physLocation == 0){
			//throw new IOException("Record not found, recid: "+recid);
			return null;
		}
		_physMgr.fetch(insertBAO, physLocation);

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


    public int getRootCount()
    {
        return FileHeader.NROOTS;
    }

    public synchronized long getRoot( int id )
        throws IOException
    {
        checkIfClosed();

        return _physPageman.getFileHeader().getRoot( id );
    }


    public synchronized void setRoot( int id, long rowid )
        throws IOException
    {
        checkIfClosed();

        _physPageman.getFileHeader().setRoot( id, rowid );
    }


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


    public synchronized void commit()
        throws IOException
    {
        checkIfClosed();
        /** flush free phys rows into pages*/
        _physMgr.commit();
        _logicMgr.commit();

        /**commit pages */
        _physPageman.commit();
        _physPagemanFree.commit();
        _logicPageman.commit();
        _logicPagemanFree.commit();
    }


    public synchronized void rollback()
        throws IOException
    {
        checkIfClosed();
        _physMgr.commit();
        _logicMgr.commit();


        _physPageman.rollback();
        _physPagemanFree.rollback();
        _logicPageman.rollback();
        _logicPagemanFree.rollback();
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
        if ( _physFile == null ) {
            throw new IllegalStateException( "RecordManager has been closed" );
        }
    }


	public synchronized void clearCache() throws IOException {
		//no cache		
	}


	public synchronized void defrag() throws IOException {
		checkIfClosed();
		commit();
		final String filename2 = _filename+"_defrag"+System.currentTimeMillis();
		final String filename1 = _filename; 
		BaseRecordManager recman2 = new BaseRecordManager(filename2);
		recman2.disableTransactions();
	
		PageCursor logicalCur = new PageCursor(_logicPageman, Magic.TRANSLATION_PAGE);
		List<Long> logicalPages = new ArrayList<Long>();		
		long last = logicalCur.next();;
		while(last!=0){
			logicalPages.add(last);
			last = logicalCur.next();			
		}
		for(long pageid:logicalPages){			
			BlockIo io = _logicFile.get(pageid); 
			TranslationPage xlatPage = TranslationPage.getTranslationPageView(io,TRANS_BLOCK_SIZE);
		
			for(int i = 0;i<_logicMgr.ELEMS_PER_PAGE;i+=1){
				int pos = TranslationPage.O_TRANS + i* TranslationPage.PhysicalRowId_SIZE;
				if(pos>Short.MAX_VALUE)
					throw new Error();
				long logicalRowId = Location.toLong(pageid,(short)pos);


				//find physical location
				long physRowId = Location.toLong(
						xlatPage.getLocationBlock((short)pos),
						xlatPage.getLocationOffset((short)pos));
				if(physRowId == 0)
					continue;

				//read from physical location
				ByteArrayOutputStream b = new ByteArrayOutputStream();
				_physMgr.fetch(b, physRowId);
				byte[] bb = b.toByteArray();
				//write to new file
				recman2.forceInsert(logicalRowId, bb);
				
			}
			_logicFile.release(io);
			recman2.commit();
			
		}
		recman2.setRoot(NAME_DIRECTORY_ROOT,getRoot(NAME_DIRECTORY_ROOT));
		recman2.commit();
		
		recman2.close();
		close();
		List<File> filesToDelete = new ArrayList<File>();
		//now rename old files 
		String[] exts = {IDF, IDR, DBF, DBR};
		for(String ext:exts){
			String f1 = filename1+ext;			
			String f2 = filename2+"_OLD"+ext;
			
			//first rename transaction log
			File f1t = new File(f1+TransactionManager.extension);
			File f2t = new File(f2+TransactionManager.extension);
			f1t.renameTo(f2t);
			filesToDelete.add(f2t);
			
			//rename data files, iterate until file exist
			for(int i=0;;i++){
				File f1d = new File(f1+"."+i);
				if(!f1d.exists()) break;
				File f2d = new File(f2+"."+i);
				f1d.renameTo(f2d);
				filesToDelete.add(f2d);
			}
		}
		
		//rename new files 
		for(String ext:exts){
			String f1 = filename2+ext;			
			String f2 = filename1+ext;
			
			//first rename transaction log
			File f1t = new File(f1+TransactionManager.extension);
			File f2t = new File(f2+TransactionManager.extension);
			f1t.renameTo(f2t);
			
			//rename data files, iterate until file exist
			for(int i=0;;i++){
				File f1d = new File(f1+"."+i);
				if(!f1d.exists()) break;
				File f2d = new File(f2+"."+i);
				f1d.renameTo(f2d);			
			}
		}
		
		for(File d:filesToDelete){
			d.delete();
		}
		
		
		reopen();
	}
	
	/**
	 * Insert data at forced logicalRowId, use only for defragmentation !! 
	 * @param logicalRowId 
	 * @param bb data
	 * @throws IOException 
	 */
	private void forceInsert(long logicalRowId, byte[] data) throws IOException {
		long physLoc = _physMgr.insert(data, 0, data.length);
		_logicMgr.forceInsert(logicalRowId, physLoc);
	}

	
	/**
	 * Compress recid from physical form (block - offset) to (block - slot). 
	 * This way resulting number is smaller and can be easyer packed with LongPacker  
	 */
	static long compressRecid(long recid){
		long block = Location.getBlock(recid);
		short offset=  Location.getOffset(recid);

		offset = (short) (offset - TranslationPage.O_TRANS);
		if(offset%8!=0)
			throw new InternalError("not 8");
		long slot = offset /8;
		if(slot<0||slot>255)
			throw new InternalError("too big slot: "+slot);
			
		return (block << 8) + (long) slot;

	}
	
	static long decompressRecid(long recid){
		long block = recid >>8;
        short offset = (short) (((recid & 0xff) ) * 8+TranslationPage.O_TRANS);
		return Location.toLong(block, offset);
	}}
