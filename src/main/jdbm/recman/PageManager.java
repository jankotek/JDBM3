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
 *  This class manages the linked lists of pages that make up a file.
 */
final class PageManager {
    // our record file
    private RecordFile file;
    // header data
    private FileHeader header;
    private BlockIo headerBuf;
    
    /**
     *  Creates a new page manager using the indicated record file.
     */
    PageManager(RecordFile file) throws IOException {
        this.file = file;
        
        // check the file header. If the magic is 0, we assume a new
        // file. Note that we hold on to the file header node.
        headerBuf = file.get(0);
        if (headerBuf.readShort(0) == 0)
            header = new FileHeader(headerBuf, true);
        else
            header = new FileHeader(headerBuf, false);
    }
    
    /**
     *  Allocates a page of the indicated type. Returns recid of the
     *  page.
     */
    long allocate(short type) throws IOException {
        
        if (type == Magic.FREE_PAGE)
            throw new Error("allocate of free page?");
        
        // do we have something on the free list?
        long retval = header.getFirstOf(Magic.FREE_PAGE);
        boolean isNew = false;
        if (retval != 0) {
            // yes. Point to it and make the next of that page the
            // new first free page.
            header.setFirstOf(Magic.FREE_PAGE, getNext(retval));
        }
        else {
            // nope. make a new record
            retval = header.getLastOf(Magic.FREE_PAGE);
            if (retval == 0)
                // very new file - allocate record #1
                retval = 1;
            header.setLastOf(Magic.FREE_PAGE, retval + 1);
            isNew = true;
        }
        
        // Cool. We have a record, add it to the correct list
        BlockIo buf = file.get(retval);
        PageHeader pageHdr = isNew ? new PageHeader(buf, type) 
            : PageHeader.getView(buf);
        long oldLast = header.getLastOf(type);
        
        // Clean data.
        System.arraycopy(file.cleanData, 0, 
                         buf.getData(), 0, 
                         file.BLOCK_SIZE);
        pageHdr.setType(type);
        pageHdr.setPrev(oldLast);
        pageHdr.setNext(0);
        
        
        if (oldLast == 0)
            // This was the first one of this type
            header.setFirstOf(type, retval);
        header.setLastOf(type, retval);
        file.release(retval, true);
        
        // If there's a previous, fix up its pointer
        if (oldLast != 0) {
            buf = file.get(oldLast);
            pageHdr = PageHeader.getView(buf);
            pageHdr.setNext(retval);
            file.release(oldLast, true);
        }
        
        // remove the view, we have modified the type.
        buf.setView(null);
        
        return retval;
    }
    
    /**
     *  Frees a page of the indicated type.
     */
    void free(short type, long recid) throws IOException {
        if (type == Magic.FREE_PAGE)
            throw new Error("free free page?");
        if (recid == 0)
            throw new Error("free header page?");
        
        // get the page and read next and previous pointers
        BlockIo buf = file.get(recid);
        PageHeader pageHdr = PageHeader.getView(buf);
        long prev = pageHdr.getPrev();
        long next = pageHdr.getNext();
        
        // put the page at the front of the free list.
        pageHdr.setType(Magic.FREE_PAGE);
        pageHdr.setNext(header.getFirstOf(Magic.FREE_PAGE));
        pageHdr.setPrev(0);
        
        header.setFirstOf(Magic.FREE_PAGE, recid);
        file.release(recid, true);
        
        // remove the page from its old list
        if (prev != 0) {
            buf = file.get(prev);
            pageHdr = PageHeader.getView(buf);
            pageHdr.setNext(next);
            file.release(prev, true);
        }
        else {
            header.setFirstOf(type, next);
        }
        if (next != 0) {
            buf = file.get(next);
            pageHdr = PageHeader.getView(buf);
            pageHdr.setPrev(prev);
            file.release(next, true);
        }
        else {
            header.setLastOf(type, prev);
        }
        
    }
    
    
    /**
     *  Returns the page following the indicated block
     */
    long getNext(long block) throws IOException {
        try {
            return PageHeader.getView(file.get(block)).getNext();
        } finally {
            file.release(block, false);
        }
    }
    
    /**
     *  Returns the page before the indicated block
     */
    long getPrev(long block) throws IOException {
        try {
            return PageHeader.getView(file.get(block)).getPrev();
        } finally {
            file.release(block, false);
        }
    }
    
    /**
     *  Returns the first page on the indicated list.
     */
    long getFirst(short type) throws IOException {
        return header.getFirstOf(type);
    }

    /**
     *  Returns the last page on the indicated list.
     */
    long getLast(short type) throws IOException {
        return header.getLastOf(type);
    }
    
    
    /**
     *  Commit all pending (in-memory) data by flushing the page manager.
     *  This forces a flush of all outstanding blocks (this it's an implicit
     *  {@link RecordFile#commit} as well).
     */
    void commit() throws IOException {
        // write the header out
        file.release(headerBuf);
        file.commit();

        // and obtain it again
        headerBuf = file.get(0);
        header = new FileHeader(headerBuf, false);
    }

    /**
     *  Flushes the page manager. This forces a flush of all outstanding
     *  blocks (this it's an implicit {@link RecordFile#commit} as well).
     */
    void rollback() throws IOException {
        // release header
        file.discard(headerBuf);
        file.rollback();
        // and obtain it again
        headerBuf = file.get(0);
        if (headerBuf.readShort(0) == 0)
            header = new FileHeader(headerBuf, true);
        else
            header = new FileHeader(headerBuf, false);
    }
    
    /**
     *  Closes the page manager. This flushes the page manager and releases
     *  the lock on the header.
     */
    void close() throws IOException {   
        file.release(headerBuf);
        file.commit();
        headerBuf = null;
        header = null;
        file = null;
    }
    
    /**
     *  Returns the file header.
     */
    FileHeader getFileHeader() {
        return header;
    }
    
}
