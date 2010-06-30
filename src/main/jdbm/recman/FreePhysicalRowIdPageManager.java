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

/**
 *  This class manages free physical rowid pages and provides methods
 *  to free and allocate physical rowids on a high level.
 */
final class FreePhysicalRowIdPageManager
{
    // our record file
    protected RecordFile _file;

    // our page manager
    protected PageManager _pageman;

    /**
     *  Creates a new instance using the indicated record file and
     *  page manager.
     */
    FreePhysicalRowIdPageManager( RecordFile file, PageManager pageman )
        throws IOException
    {
        _file = file;
        _pageman = pageman;
    }


    /**
	 * Returns a free physical rowid of the indicated size, or null if nothing
	 * was found. This scans the free physical row table, which is modeled as a
	 * linked list of pages. Each page on that list has slots that are either
	 * free (awaiting the insertion of the location of a free physical row) or
	 * available for reallocation requests. An allocated slot is indicated by a
	 * non-zero size field in that slot and the size is the size of the
	 * available free record in bytes.
	 */
    Location get( int size )
        throws IOException
    {
        // Loop through the free physical rowid list until we find
        // a rowid that's large enough.
        Location retval = null;
        PageCursor curs = new PageCursor( _pageman, Magic.FREEPHYSIDS_PAGE );

        while (curs.next() != 0) {
            FreePhysicalRowIdPage fp = FreePhysicalRowIdPage
                .getFreePhysicalRowIdPageView( _file.get( curs.getCurrent() ) );
            int slot = fp.getFirstLargerThan( size );
            if ( slot != -1 ) {
                // got one!
                retval = fp.slotToLocation(slot);

                fp.free( slot );
                if ( fp.getCount() == 0 ) {
                    // page became empty - free it
                    _file.release( curs.getCurrent(), false );
                    _pageman.free( Magic.FREEPHYSIDS_PAGE, curs.getCurrent() );
                } else {
                    _file.release( curs.getCurrent(), true );
                }

                return retval;
            } else {
                // no luck, go to next page
                _file.release( curs.getCurrent(), false );
            }

        }
        return null;
    }

    /**
     *  Puts the indicated rowid on the free list
     */
    void put(Location rowid, int size)
  throws IOException {

  short freePhysRowId = -1;
  FreePhysicalRowIdPage fp = null;
  PageCursor curs = new PageCursor(_pageman, Magic.FREEPHYSIDS_PAGE);
  long freePage = 0;
  while (curs.next() != 0) {
      freePage = curs.getCurrent();
      BlockIo curBlock = _file.get(freePage);
      fp = FreePhysicalRowIdPage
    .getFreePhysicalRowIdPageView(curBlock);
      int slot = fp.getFirstFree();
      if (slot != -1) {
    	  freePhysRowId = fp.alloc(slot);
    break;
      }

      _file.release(curBlock);
  }
  if (freePhysRowId == -1) {
      // No more space on the free list, add a page.
      freePage = _pageman.allocate(Magic.FREEPHYSIDS_PAGE);
      BlockIo curBlock = _file.get(freePage);
      fp =
    FreePhysicalRowIdPage.getFreePhysicalRowIdPageView(curBlock);
      freePhysRowId = fp.alloc(0);
  }
  fp.PhysicalRowId_setBlock(freePhysRowId, rowid.getBlock());
  fp.PhysicalRowId_setOffset(freePhysRowId, rowid.getOffset());
  fp.FreePhysicalRowId_setSize(freePhysRowId, size);
  _file.release(freePage, true);
    }
}
