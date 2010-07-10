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

/**
 *  Class describing a page that holds translations from physical rowids
 *  to logical rowids. In fact, the page just holds physical rowids - the
 *  page's block is the block for the logical rowid, the offset serve
 *  as offset for the rowids.
 */
final class TranslationPage extends PageHeader {
    // offsets
    static final short O_TRANS = PageHeader.SIZE; // short count
    
    
//    // slots we returned.
//    final PhysicalRowId[] slots = new PhysicalRowId[ELEMS_PER_PAGE];

    /**
     *  Constructs a data page view from the indicated block.
     */
    TranslationPage(BlockIo block, int blockSize) {
        super(block);
        
    }

    /**
     *  Factory method to create or return a data page for the
     *  indicated block.
     */
    static TranslationPage getTranslationPageView(BlockIo block, int blockSize) {
        BlockView view = block.getView();
        if (view != null && view instanceof TranslationPage)
            return (TranslationPage) view;
        else
            return new TranslationPage(block, blockSize);
    }

//    /** Returns the value of the indicated rowid on the page */
//    short get(short offset) {
//        int slot = (offset - O_TRANS) / PhysicalRowId.SIZE;
//        if (slots[slot] == null) 
//            slots[slot] = new PhysicalRowId(block, offset);
//        return slots[slot];
//    }
}
