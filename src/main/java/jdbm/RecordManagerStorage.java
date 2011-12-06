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


import javax.rmi.CORBA.Util;
import java.io.*;
import java.util.*;

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
 * @author Alex Boisvert
 * @author Cees de Groot
 */
final class RecordManagerStorage
    extends RecordManager2
{

	private static final String IDR = ".idr";

	private static final String DBR = ".dbr";

	
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
     * Indicated that store is opened for readonly operations
     * If true, store will throw UnsupportedOperationException when update/insert/delete operation is called
     */
    private boolean readonly = false;

    void setReadonly(boolean readonly){
        this.readonly = readonly;
    }

    void checkCanWrite(){
        if(readonly)
            throw new UnsupportedOperationException("Could not write, store is opened as read-only");
    }


    /** if true, new records alwayes saved to end of file
     * and free space is not reclaimed.
     * This may speed up some operations which involves lot of
     * updates and inserts (batch creation);
     * You need to reopen store to apply effect
     */
    public boolean isAppendToEnd() {
        return appendToEnd;
    }
    /** if true, new records alwayes saved to end of file
     * and free space is not reclaimed.
     * This may speed up some operations which involves lot of
     * updates and inserts (batch creation);
     *
     * You need to reopen store to apply effect
     */
    public void setAppendToEnd(boolean appendToEnd) {
        this.appendToEnd = appendToEnd;
    }

    /** if true, new records alwayes saved to end of file
     * and free space is not reclaimed.
     * This may speed up some operations which involves lot of
     * updates and inserts (batch creation);
      */
    private boolean appendToEnd = false;



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
     * Reserved slot for name directory recid.
     */
    public static final int NAME_DIRECTORY_ROOT = 0;


    /**
     * Reserved slot for version number
     */
    public static final int STORE_VERSION_NUMBER_ROOT = 1;

    /**
     * Reserved slot for recid where Serial class info is stored
     */
    public static final int SERIAL_CLASS_INFO_RECID_ROOT = 2;



	private final byte[] _insertBuffer = new byte[RecordFile.BLOCK_SIZE];
	private final Utils.OpenByteArrayOutputStream _insertBAO = new Utils.OpenByteArrayOutputStream(_insertBuffer);
	private final Utils.SerializerOutput _insertOut = new Utils.SerializerOutput(_insertBAO);
	private final Utils.OpenByteArrayInputStream _insertBAI = new Utils.OpenByteArrayInputStream(_insertBuffer);
	private final DataInputStream _insertIn = new DataInputStream(_insertBAI);

	volatile private boolean bufferInUse = false;


	private final String _filename; 


    /**
     *  Creates a record manager for the indicated file
     *
     *  @throws IOException when the file cannot be opened or is not
     *          a valid file content-wise.
     */
    public RecordManagerStorage(String filename)
        throws IOException
    {
    	_filename = filename;
    	reopen();
    }


    private void reopen() throws IOException {
        _physFile = new RecordFile( _filename + DBR);
        _physPageman = new PageManager( _physFile );
        _physMgr = new PhysicalRowIdManager( _physFile, _physPageman, 
        		new FreePhysicalRowIdPageManager(_physFile, _physPageman,appendToEnd));
        
        if(RecordFile.BLOCK_SIZE >256*8)
        	throw new InternalError(); //too big page, slot number would not fit into page
        _logicFile = new RecordFile( _filename +IDR);
        _logicPageman = new PageManager( _logicFile );
        _logicMgr = new LogicalRowIdManager( _logicFile, _logicPageman, 
        		new FreeLogicalRowIdPageManager(_physFile, _physPageman));

        long versionNumber = getRoot(STORE_VERSION_NUMBER_ROOT);
        if(versionNumber>STORE_FORMAT_VERSION)
        	throw new IOException("Unsupported version of store. Please update JDBM. Minimal supported ver:"+STORE_FORMAT_VERSION+", store ver:"+versionNumber);
        setRoot(STORE_VERSION_NUMBER_ROOT, STORE_FORMAT_VERSION);

        defaultSerializer = null;

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
        checkCanWrite();
    	if(bufferInUse){
    		//current reusable buffer is in use, have to fallback into creating new instances
    		byte[] buffer = new byte[1024];
    		Utils.OpenByteArrayOutputStream bao = new Utils.OpenByteArrayOutputStream(buffer);
    		Utils.SerializerOutput out = new Utils.SerializerOutput(bao);
    		return insert2(obj,serializer,buffer,bao,out);
    	}

        try{
        		
        	bufferInUse = true;
                _insertOut.__resetWrittenCounter();
        	return insert2(obj, serializer,_insertBuffer,_insertBAO,_insertOut);
        }finally{
        	bufferInUse = false;
        }
    }


	private <A> long insert2(A obj, Serializer<A> serializer, byte[] insertBuffer, Utils.OpenByteArrayOutputStream insertBAO, Utils.SerializerOutput insertOut)
			throws IOException {
		insertBAO.reset(insertBuffer);
      
		serializer.serialize(insertOut, obj);
		long physRowId = _physMgr.insert( insertBAO.getBuf(), 0, insertBAO.size() );
		long recid = _logicMgr.insert( physRowId );
		if ( DEBUG ) {
			System.out.println( "BaseRecordManager.insert() recid " + recid + " length " + insertBAO.size() ) ;
		}
		return compressRecid(recid);
	}


    public synchronized void delete( long logRowId )
        throws IOException
    {
    	
        checkIfClosed();
        checkCanWrite();
        if ( logRowId <= 0 ) {
            throw new IllegalArgumentException( "Argument 'recid' is invalid: "
                                                + logRowId );
        }

        if ( DEBUG ) {
            System.out.println( "BaseRecordManager.delete() recid " + logRowId ) ;
        }

        logRowId = decompressRecid(logRowId);
        
        long physRowId = _logicMgr.fetch(logRowId);
        _physMgr.delete( physRowId );
        _logicMgr.delete( logRowId );
    }



    public synchronized  <A> void update( long recid, A obj, Serializer<A> serializer )
        throws IOException
    {
        checkIfClosed();
        checkCanWrite();
        if ( recid <= 0 ) {
            throw new IllegalArgumentException( "Argument 'recid' is invalid: "
                                                + recid );
        }
    	if(bufferInUse){
    		//current reusable buffer is in use, have to create new instances
    		byte[] buffer = new byte[1024];
    		Utils.OpenByteArrayOutputStream bao = new Utils.OpenByteArrayOutputStream(buffer);
    		Utils.SerializerOutput out = new Utils.SerializerOutput(bao);
    		update2(recid,obj,serializer,buffer,bao,out);
    		return;
    	}

        try{        
        	bufferInUse = true;
        	_insertOut.__resetWrittenCounter();
        	update2(recid, obj, serializer,_insertBuffer, _insertBAO, _insertOut);
        }finally{
        	bufferInUse = false;
        }
    }


	private <A> void update2(long logRecid, A obj, Serializer<A> serializer,byte[] insertBuffer, Utils.OpenByteArrayOutputStream insertBAO, Utils.SerializerOutput insertOut)
			throws IOException {
		logRecid = decompressRecid(logRecid);
		long physRecid = _logicMgr.fetch( logRecid );
		if(physRecid == 0)
			throw new IOException("Can not update, recid does not exist: "+logRecid);
		insertBAO.reset(insertBuffer);
		serializer.serialize(insertOut, obj );


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
    		Utils.OpenByteArrayOutputStream bao = new Utils.OpenByteArrayOutputStream(buffer);
    		Utils.SerializerOutput out = new Utils.SerializerOutput(bao);
    		Utils.OpenByteArrayInputStream bai = new Utils.OpenByteArrayInputStream(buffer);
    		DataInputStream in = new DataInputStream(bai);
    		return fetch2(recid,serializer,buffer,bao,out, bai,in);
    	}
        try{
        	bufferInUse = true;
                _insertOut.__resetWrittenCounter();
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
				Utils.OpenByteArrayOutputStream insertBAO, Utils.SerializerOutput insertOut,
				Utils.OpenByteArrayInputStream insertBAI, DataInputStream insertIn)
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
        		return serializer.deserialize( insertIn );
		}catch(ClassNotFoundException e){
			throw new IOError(e);
		}
	}

    byte[] fetchRaw(long recid) throws IOException {
        recid = decompressRecid(recid);
        long physLocation = _logicMgr.fetch(recid);
        if(physLocation == 0){
                //throw new IOException("Record not found, recid: "+recid);
                return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        _physMgr.fetch(out, physLocation);
        return out.toByteArray();
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
        checkCanWrite();

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
        checkCanWrite();

        Map<String,Long> nameDirectory = getNameDirectory();
        if ( recid == 0 ) {
            // remove from hashtable
            nameDirectory.remove( name );
        } else {
            nameDirectory.put( name, new Long( recid ) );
        }
        saveNameDirectory( nameDirectory );
    }


    private Serialization defaultSerializer;

    public synchronized Serializer defaultSerializer() {
        if(defaultSerializer == null) try{
            long serialClassInfoRecid = getRoot(SERIAL_CLASS_INFO_RECID_ROOT);
            if(serialClassInfoRecid == 0){
                //insert new empty array list
                serialClassInfoRecid = insert(new ArrayList<SerialClassInfo.ClassInfo>(0),SerialClassInfo.serializer);
                setRoot(SERIAL_CLASS_INFO_RECID_ROOT,serialClassInfoRecid);
            }

            defaultSerializer = new Serialization(this,serialClassInfoRecid);
        }catch(IOException e){
            throw new IOError(e);
        }
        return defaultSerializer;
    }


    public synchronized void commit()
        throws IOException
    {
        checkIfClosed();
        checkCanWrite();
        /** flush free phys rows into pages*/
        _physMgr.commit();
        _logicMgr.commit();

        /**commit pages */
        _physPageman.commit();
        _logicPageman.commit();
    }


    public synchronized void rollback()
        throws IOException
    {
        checkIfClosed();
        _physMgr.commit();
        _logicMgr.commit();

        _physPageman.rollback();
        _logicPageman.rollback();
        defaultSerializer = null;
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
        checkCanWrite();
        long recid = getRoot(NAME_DIRECTORY_ROOT);
        if ( recid == 0 ) {
            throw new IOException( "Name directory must exist" );
        }
        update(recid, _nameDirectory);
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


    private long statisticsCountPages(short pageType) throws IOException {
        long pageCounter = 0;

        for(long pageid = _logicPageman.getFirst(pageType);
              pageid!=0;
              pageid= _logicPageman.getNext(pageid)
        ){
          pageCounter++;
        }

        for(long pageid = _physPageman.getFirst(pageType);
              pageid!=0;
              pageid= _physPageman.getNext(pageid)
        ){
          pageCounter++;
        }

        return pageCounter;

    }

    public synchronized String calculateStatistics(){
        try{

            final StringBuilder b = new StringBuilder();

            //count pages
            {

               b.append("PAGES:\n");
               long total = 0;
               long pages = statisticsCountPages(Magic.USED_PAGE);
               total+=pages;
               b.append("  "+pages+" used pages with size "+Utils.formatSpaceUsage(pages*RecordFile.BLOCK_SIZE)+"\n");
               pages = statisticsCountPages(Magic.TRANSLATION_PAGE);
               total+=pages;
               b.append("  "+pages+" record translation pages with size "+Utils.formatSpaceUsage(pages*RecordFile.BLOCK_SIZE)+"\n");
               pages = statisticsCountPages(Magic.FREE_PAGE);
               total+=pages;
               b.append("  "+pages+" free (unused) pages with size "+Utils.formatSpaceUsage(pages*RecordFile.BLOCK_SIZE)+"\n");
               pages = statisticsCountPages(Magic.FREEPHYSIDS_PAGE);
               total+=pages;
               b.append("  "+pages+" free (phys) pages with size "+Utils.formatSpaceUsage(pages*RecordFile.BLOCK_SIZE)+"\n");
               pages = statisticsCountPages(Magic.FREELOGIDS_PAGE);
               total+=pages;
               b.append("  "+pages+" free (logical) pages with size "+Utils.formatSpaceUsage(pages*RecordFile.BLOCK_SIZE)+"\n");
               b.append("  Total number of pages is "+total+" with size "+Utils.formatSpaceUsage(total*RecordFile.BLOCK_SIZE)+"\n");

            }
            {
            b.append("RECORDS:\n");

            long recordCount = 0;
            long freeRecordCount = 0;
            long maximalRecordSize = 0;
            long maximalAvailSizeDiff = 0;
            long totalRecordSize = 0;
            long totalAvailDiff = 0;

            //count records
            for(long pageid = _logicPageman.getFirst(Magic.TRANSLATION_PAGE);
                pageid!=0;
                pageid= _logicPageman.getNext(pageid)
                ){
                BlockIo io = _logicFile.get(pageid);
                TranslationPage xlatPage = TranslationPage.getTranslationPageView(io);

                for(int i = 0;i<_logicMgr.ELEMS_PER_PAGE;i+=1){
                    final int pos = TranslationPage.O_TRANS + i* TranslationPage.PhysicalRowId_SIZE;
				    long physPage = xlatPage.getLocationBlock((short)pos);
                    short physOffset = xlatPage.getLocationOffset((short)pos);
				    if(physPage == 0 && physOffset == 0){
				        freeRecordCount++;
                        continue;
                    }

                    recordCount++;

                    //get size
                    BlockIo block = _physFile.get(physPage);
                    int availSize = RecordHeader.getAvailableSize(block,physOffset);
                    int currentSize = RecordHeader.getCurrentSize(block, physOffset);
                    _physFile.release(block);

                    maximalAvailSizeDiff = Math.max(maximalAvailSizeDiff, availSize-currentSize);
                    maximalRecordSize = Math.max(maximalRecordSize,currentSize);
                    totalAvailDiff +=availSize-currentSize;
                    totalRecordSize+=currentSize;

                }
            }

            b.append("  Contains "+recordCount+" records and "+freeRecordCount+" free slots.\n");
            b.append("  Total space occupied by data is "+totalRecordSize+"\n");
            b.append("  Average data size in record is "+ Utils.formatSpaceUsage(Math.round(1D*totalRecordSize/recordCount))+"\n");
            b.append("  Maximal data size in record is "+ Utils.formatSpaceUsage(maximalRecordSize)+"\n");
            b.append("  Space wasted in record fragmentation is "+ Utils.formatSpaceUsage(totalAvailDiff)+"\n");
            b.append("  Maximal space wasted in single record fragmentation is "+ Utils.formatSpaceUsage(maximalAvailSizeDiff)+"\n");
            }

            return b.toString();
        }catch(IOException e){
            throw new IOError(e);
        }
    }

	public synchronized void defrag() throws IOException {

		checkIfClosed();
                checkCanWrite();
		commit();
		final String filename2 = _filename+"_defrag"+System.currentTimeMillis();
		final String filename1 = _filename;
		RecordManagerStorage recman2 = new RecordManagerStorage(filename2);
		recman2.disableTransactions();

                //recreate logical file with original page layout
                {
                    //find maximal logical pageid
                    LongHashMap<String> logicalPages = new LongHashMap<String>();
                    long maxpageid = 0;
                    for(long pageid = _logicPageman.getFirst(Magic.TRANSLATION_PAGE);
                        pageid!=0;
                        pageid= _logicPageman.getNext(pageid)
                    ){
                        maxpageid = Math.max(maxpageid,pageid);
                        logicalPages.put(pageid, Utils.EMPTY_STRING);
                    }

                    //fill second recman with logical pages
                    for(
                      long pageid = recman2._logicPageman.allocate(Magic.TRANSLATION_PAGE);
                      pageid<=maxpageid;
                      pageid = recman2._logicPageman.allocate(Magic.TRANSLATION_PAGE)
                    ){}

                    //free pages which are not actually logical in second recman
                    for(long pageid = recman2._logicPageman.getFirst(Magic.TRANSLATION_PAGE);
                        pageid<=maxpageid;
                        pageid += RecordFile.BLOCK_SIZE
                    ){
                        if(logicalPages.get(pageid)==null){
                            recman2._logicPageman.free(Magic.TRANSLATION_PAGE,Magic.TRANSLATION_PAGE);
                        }
                    }
                    logicalPages = null;
                }



                //reinsert collections so physical records are located near each other
                //iterate over named object recids, it is sorted with TreeSet
                for(Long namedRecid : new TreeSet<Long>(getNameDirectory().values()) ){
                    Object obj = fetch(namedRecid);
                    if(obj instanceof LinkedList){
                        LinkedList.defrag(namedRecid, this, recman2);
                    }

                }


                for(long pageid = _logicPageman.getFirst(Magic.TRANSLATION_PAGE);
                    pageid!=0;
                    pageid= _logicPageman.getNext(pageid)
                    ){
			BlockIo io = _logicFile.get(pageid);
			TranslationPage xlatPage = TranslationPage.getTranslationPageView(io);

			for(int i = 0;i<_logicMgr.ELEMS_PER_PAGE;i+=1){
				final int pos = TranslationPage.O_TRANS + i* TranslationPage.PhysicalRowId_SIZE;
				if(pos>Short.MAX_VALUE)
					throw new Error();

				//write to new file
                                final long logicalRowId = Location.toLong(pageid,(short)pos);

                                //read from logical location in second recman,
                                //check if record was already inserted as part of collections
                                if( recman2._logicPageman.getLast(Magic.TRANSLATION_PAGE)>=pageid &&
                                        recman2._logicMgr.fetch(logicalRowId)!=0){
                                    //yes, this record already exists in second recman
                                    continue;
                                }

				//get physical location in this recman
				long physRowId = Location.toLong(
						xlatPage.getLocationBlock((short)pos),
						xlatPage.getLocationOffset((short)pos));
				if(physRowId == 0)
					continue;

				//read from physical location at this recman
				ByteArrayOutputStream b = new ByteArrayOutputStream();
				_physMgr.fetch(b, physRowId);
				byte[] bb = b.toByteArray();

                                //force insert into other file, without decompressing logical id to external form
                                long physLoc = recman2._physMgr.insert(bb, 0, bb.length);
                                recman2._logicMgr.forceInsert(logicalRowId, physLoc);

			}
			_logicFile.release(io);
		}
		recman2.setRoot(NAME_DIRECTORY_ROOT,getRoot(NAME_DIRECTORY_ROOT));

		recman2.close();
		close();

		List<File> filesToDelete = new ArrayList<File>();
		//now rename old files
		String[] exts = {IDR, DBR};
		for(String ext:exts){
			String f1 = filename1+ext;
			String f2 = filename2+"_OLD"+ext;

			//first rename transaction log
			File f1t = new File(f1+StorageDisk.transaction_log_file_extension);
			File f2t = new File(f2+StorageDisk.transaction_log_file_extension);
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
			File f1t = new File(f1+StorageDisk.transaction_log_file_extension);
			File f2t = new File(f2+StorageDisk.transaction_log_file_extension);
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
	 * @param data
	 * @throws IOException 
	 */
	void forceInsert(long logicalRowId, byte[] data) throws IOException {
                logicalRowId = decompressRecid(logicalRowId);
		long physLoc = _physMgr.insert(data, 0, data.length);
		_logicMgr.forceInsert(logicalRowId, physLoc);
	}


	/**
	 * Compress recid from physical form (block - offset) to (block - slot). 
	 * This way resulting number is smaller and can be easier packed with LongPacker
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
	}

    /**
     * Returns number of records stored in database.
     * Is used for unit tests
     */
     long countRecords() throws IOException {
         long counter = 0;

         long page = _logicPageman.getFirst(Magic.TRANSLATION_PAGE);
         while(page!=0){
             BlockIo io = _logicFile.get(page);
             TranslationPage xlatPage = TranslationPage.getTranslationPageView(io);
             for(int i = 0;i<_logicMgr.ELEMS_PER_PAGE;i+=1){
                     int pos = TranslationPage.O_TRANS + i* TranslationPage.PhysicalRowId_SIZE;
                     if(pos>Short.MAX_VALUE)
                             throw new Error();

                     //get physical location
                     long physRowId = Location.toLong(
                                     xlatPage.getLocationBlock((short)pos),
                                     xlatPage.getLocationOffset((short)pos));
                     if(physRowId != 0)
                             counter+=1;
             }
             _logicFile.release(io);
             page = _logicPageman.getNext(page);
         }
         return counter;
     }


}
