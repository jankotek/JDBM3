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
 *  This class manages the linked lists of logical rowid pages.
 */
final class LogicalRowIdManager {
    // our record file and associated page manager
    private RecordFile file;
    private PageManager pageman;
    private FreeLogicalRowIdPageManager freeman;

    /**
     *  Creates a log rowid manager using the indicated record file and
     *  page manager
     */
    LogicalRowIdManager(RecordFile file, PageManager pageman)
  throws IOException {
  this.file = file;
  this.pageman = pageman;
  this.freeman = new FreeLogicalRowIdPageManager(file, pageman);

    }

    /**
     *  Creates a new logical rowid pointing to the indicated physical
     *  id
     */
    Location insert(Location loc)
    throws IOException {
  // check whether there's a free rowid to reuse
  Location retval = freeman.get();
  if (retval == null) {
      // no. This means that we bootstrap things by allocating
      // a new translation page and freeing all the rowids on it.
      long firstPage = pageman.allocate(Magic.TRANSLATION_PAGE);
      short curOffset = TranslationPage.O_TRANS;
      for (int i = 0; i < TranslationPage.ELEMS_PER_PAGE; i++) {
    freeman.put(new Location(firstPage, curOffset));
    curOffset += PhysicalRowId.SIZE;
      }
      retval = freeman.get();
      if (retval == null) {
    throw new Error("couldn't obtain free translation");
      }
  }
  // write the translation.
  update(retval, loc);
  return retval;
    }

    /**
     *  Releases the indicated logical rowid.
     */
    void delete(Location rowid)
  throws IOException {

  freeman.put(rowid);
    }

    /**
     *  Updates the mapping
     *
     *  @param rowid The logical rowid
     *  @param loc The physical rowid
     */
    void update(Location rowid, Location loc)
    throws IOException {

        TranslationPage xlatPage = TranslationPage.getTranslationPageView(
                                       file.get(rowid.getBlock()));
        PhysicalRowId physid = xlatPage.get(rowid.getOffset());
        physid.setBlock(loc.getBlock());
        physid.setOffset(loc.getOffset());
        file.release(rowid.getBlock(), true);
    }

    /**
     *  Returns a mapping
     *
     *  @param rowid The logical rowid
     *  @return The physical rowid
     */
    Location fetch(Location rowid)
    throws IOException {

        TranslationPage xlatPage = TranslationPage.getTranslationPageView(
                                       file.get(rowid.getBlock()));
        try {
            Location retval = new Location(xlatPage.get(rowid.getOffset()));
            return retval;
        } finally {
            file.release(rowid.getBlock(), false);
        }
    }

}
