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

import java.io.IOException;
import java.io.OutputStream;

/**
 *  This class manages physical row ids, and their data.
 */
final class PhysicalRowIdManager
{

    // The file we're talking to and the associated page manager.
    private final RecordFile file;
    private final PageManager pageman;
    private final FreePhysicalRowIdPageManager freeman;
    private final int blockSize;
   

    /**
     *  Creates a new rowid manager using the indicated record file.
     *  and page manager.
     */
    PhysicalRowIdManager( RecordFile file, PageManager pageManager,FreePhysicalRowIdPageManager freeman )
        throws IOException
    {
        this.file = file;
        this.pageman = pageManager;
        this.freeman = freeman;
        this.blockSize = file.BLOCK_SIZE;
    }

    /**
     *  Inserts a new record. Returns the new physical rowid.
     */
    long insert( byte[] data, int start, int length )
        throws IOException
    {
    	if(length <1)
    		throw new IllegalArgumentException("Lenght is <1");
    	if(start <0)
    		throw new IllegalArgumentException("negative start");
    	
        long retval = alloc( length );
        write( retval, data, start, length );
        return retval;
    }

    /**
     *  Updates an existing record. Returns the possibly changed
     *  physical rowid.
     */
    long update( long rowid, byte[] data, int start, int length )
        throws IOException
    {
        // fetch the record header
        BlockIo block = file.get( Location.getBlock(rowid));
        //RecordHeader head = new RecordHeader( block, rowid.getOffset() );
        int availSize = RecordHeader.getAvailableSize(block, Location.getOffset(rowid));
        if (// not enough space - we need to copy to a new rowid.
        		length > availSize 
        		||
        	//this would create too big free space, move to new location
        		availSize > RecordHeader.MAX_SIZE_SPACE + length
        		) {

            file.release( block );
            free( rowid );
            rowid = alloc( length );
        } else {
            file.release( block );
        }

        // 'nuff space, write it in and return the rowid.
        write( rowid, data, start, length );
        return rowid;
    }

    /**
     *  Deletes a record.
     */
    void delete( long rowid )
        throws IOException
    {
        free( rowid );
    }

    /**
     *  Retrieves a record.
     */
//    byte[] fetch( Location rowid )
//        throws IOException 
//    {
//        // fetch the record header
//        PageCursor curs = new PageCursor( pageman, rowid.getBlock() );
//        BlockIo block = file.get( curs.getCurrent() );
//        RecordHeader head = new RecordHeader( block, rowid.getOffset() );
//
//        // allocate a return buffer
//        byte[] retval = new byte[ head.getCurrentSize() ];
//        if ( retval.length == 0 ) {
//            file.release( curs.getCurrent(), false );
//            return retval;
//        }
//
//        // copy bytes in
//        int offsetInBuffer = 0;
//        int leftToRead = retval.length;
//        short dataOffset = (short) (rowid.getOffset() + RecordHeader.SIZE);
//        while ( leftToRead > 0 ) {
//            // copy current page's data to return buffer
//            int toCopy = RecordFile.BLOCK_SIZE - dataOffset;
//            if ( leftToRead < toCopy ) {
//                toCopy = leftToRead;
//            }
//            System.arraycopy( block.getData(), dataOffset,
//                              retval, offsetInBuffer,
//                              toCopy );
//
//            // Go to the next block
//            leftToRead -= toCopy;
//            offsetInBuffer += toCopy;
//
//            file.release( block );
//
//            if ( leftToRead > 0 ) {
//                block = file.get( curs.next() );
//                dataOffset = DataPage.O_DATA;
//            }
//
//        }
//
//        return retval;
//    }
    
  void fetch(OutputStream out, long rowid )
  throws IOException 
{
  // fetch the record header
  PageCursor curs = new PageCursor( pageman, Location.getBlock(rowid));
  BlockIo block = file.get( curs.getCurrent() );
  //RecordHeader head = new RecordHeader( block, rowid.getOffset() );

  // allocate a return buffer
  //byte[] retval = new byte[ head.getCurrentSize() ];
  final int size = RecordHeader.getCurrentSize( block, Location.getOffset(rowid));
  if ( size == 0 ) {
      file.release( curs.getCurrent(), false );
      return;
  }

  // copy bytes in
  int offsetInBuffer = 0;
  int leftToRead = size;
  short dataOffset = (short) (Location.getOffset(rowid) + RecordHeader.SIZE);
  while ( leftToRead > 0 ) {
      // copy current page's data to return buffer
      int toCopy = blockSize - dataOffset;
      if ( leftToRead < toCopy ) {
          toCopy = leftToRead;
      }
      byte[] blockData = block.getData();
      int finish =dataOffset+toCopy;
      out.write(blockData,dataOffset,finish-dataOffset);

      // Go to the next block
      leftToRead -= toCopy;
      offsetInBuffer += toCopy;
//      out.flush();
      file.release( block );
      
      if ( leftToRead > 0 ) {
          block = file.get( curs.next() );
          dataOffset = DataPage.O_DATA;
      }

  }

  //return retval;
}
    

    /**
     *  Allocate a new rowid with the indicated size.
     */
    private long alloc( int size )
        throws IOException
    {    
        long retval = freeman.get( size );
        if ( retval == 0 ) {
        	size = RecordHeader.roundAvailableSize(size);
            retval = allocNew( size, pageman.getLast( Magic.USED_PAGE ) );
        }
        return retval;
    }

    /**
     *  Allocates a new rowid. The second parameter is there to
     *  allow for a recursive call - it indicates where the search
     *  should start.
     */
    private long allocNew( int size, long start )
        throws IOException
    {
        BlockIo curBlock;
        DataPage curPage;
        if ( start == 0 ) {
            // we need to create a new page.
            start = pageman.allocate( Magic.USED_PAGE );
            curBlock = file.get( start );
            curPage = DataPage.getDataPageView( curBlock, blockSize );
            curPage.setFirst( DataPage.O_DATA );
            RecordHeader.setAvailableSize(curBlock, DataPage.O_DATA, 0 );
            RecordHeader.setCurrentSize(curBlock, DataPage.O_DATA, 0 );
        } else {
            curBlock = file.get( start );
            curPage = DataPage.getDataPageView( curBlock, blockSize );
        }

        // follow the rowids on this page to get to the last one. We don't
        // fall off, because this is the last page, remember?
        short pos = curPage.getFirst();
        if ( pos == 0 ) {
            // page is exactly filled by the last block of a record
            file.release( curBlock );
            return allocNew( size, 0 );
        }
//        RecordHeader hdr = new RecordHeader( curBlock, pos );
        while ( RecordHeader.getAvailableSize(curBlock,pos) != 0 && pos < blockSize ) {
            pos += RecordHeader.getAvailableSize(curBlock,pos) + RecordHeader.SIZE;
            if ( pos == blockSize ) {
                // Again, a filled page.
                file.release( curBlock );
                return allocNew( size, 0 );
            }

//            hdr = new RecordHeader( curBlock, pos );
        }

        if ( pos == RecordHeader.SIZE ) {
            // the last record exactly filled the page. Restart forcing
            // a new page.
            file.release( curBlock );
        }

        // we have the position, now tack on extra pages until we've got
        // enough space.
        long retval = Location.toLong( start, pos );
        int freeHere = blockSize - pos - RecordHeader.SIZE;
        if ( freeHere < size ) {
            // check whether the last page would have only a small bit left.
            // if yes, increase the allocation. A small bit is a record
            // header plus 16 bytes.
        	// note: size can be linearly increased only at first multiplyer level        	
        	if(size<RecordHeader.base1-100){ //make sure page rounding does not overflow
        		int lastSize = (size - freeHere) % blockSize;
        		if (( blockSize - lastSize ) < (RecordHeader.SIZE + 16) ) {
        			size += (blockSize - lastSize); 
        		}
        	}

            // write out the header now so we don't have to come back.
        	RecordHeader.setAvailableSize(curBlock, pos,size );
            file.release( start, true );

            int neededLeft = size - freeHere;
            // Refactor these two blocks!
            while ( neededLeft >= blockSize ) {
                start = pageman.allocate( Magic.USED_PAGE );
                curBlock = file.get( start );
                curPage = DataPage.getDataPageView( curBlock, blockSize );
                curPage.setFirst( (short) 0 ); // no rowids, just data
                file.release( start, true );
                neededLeft -= curPage.DATA_PER_PAGE;
            }
            if ( neededLeft > 0 ) {
                // done with whole chunks, allocate last fragment.
                start = pageman.allocate( Magic.USED_PAGE );
                curBlock = file.get( start );
                curPage = DataPage.getDataPageView( curBlock,blockSize );
                curPage.setFirst( (short) (DataPage.O_DATA + neededLeft) );
                file.release( start, true );
            }
        } else {
            // just update the current page. If there's less than 16 bytes
            // left, we increase the allocation (16 bytes is an arbitrary
            // number).
        	// note: size can be linearly increased only at first multiplyer level
        	if(size<10000){ //make page rounding does not overflow 
        		if ( freeHere - size <= (16 + RecordHeader.SIZE) ) {
        			size = freeHere;
        		}
        	}
            RecordHeader.setAvailableSize(curBlock,pos, size );
            file.release( start, true );
        }
        return retval;

    }


    private void free( long id )
        throws IOException
    {
        // get the rowid, and write a zero current size into it.
        BlockIo curBlock = file.get(Location.getBlock(id));
        DataPage curPage = DataPage.getDataPageView( curBlock,blockSize );
//        RecordHeader hdr = new RecordHeader( curBlock, id.getOffset() );
        RecordHeader.setCurrentSize(curBlock, Location.getOffset(id), 0 );
        file.release( Location.getBlock(id), true );

        // write the rowid to the free list
        freeman.put( id, RecordHeader.getAvailableSize( curBlock, Location.getOffset(id)) );
    }

    /**
     *  Writes out data to a rowid. Assumes that any resizing has been
     *  done.
     */
    private void write(long rowid, byte[] data, int start, int length )
        throws IOException
    {
        PageCursor curs = new PageCursor( pageman, Location.getBlock(rowid) );
        BlockIo block = file.get( curs.getCurrent() );
        //RecordHeader hdr = new RecordHeader( block, rowid.getOffset() );
        RecordHeader.setCurrentSize(block, Location.getOffset(rowid), length );
        if ( length == 0 ) {
            file.release( curs.getCurrent(), true );
            return;
        }

        // copy bytes in
        int offsetInBuffer = start;
        int leftToWrite = length;
        short dataOffset = (short) (Location.getOffset(rowid) + RecordHeader.SIZE);
        while ( leftToWrite > 0 ) {
            // copy current page's data to return buffer
            int toCopy = blockSize - dataOffset;

            if ( leftToWrite < toCopy ) {
                toCopy = leftToWrite;
            }
            
            System.arraycopy( data, offsetInBuffer, block.getData(), 
                              dataOffset, toCopy );

            // Go to the next block
            leftToWrite -= toCopy;
            offsetInBuffer += toCopy;

            file.release( curs.getCurrent(), true );

            if ( leftToWrite > 0 ) {
                block = file.get( curs.next() );
                dataOffset = DataPage.O_DATA;
            }
        }
    }
}

