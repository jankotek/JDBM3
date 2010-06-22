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
 *  This class provides a cursor that can follow lists of pages
 *  bi-directionally.
 */
final class PageCursor {
    PageManager pageman;
    long current;
    short type;
    
    /**
     *  Constructs a page cursor that starts at the indicated block.
     */
    PageCursor(PageManager pageman, long current) {
        this.pageman = pageman;
        this.current = current;
    }
    
    /**
     *  Constructs a page cursor that starts at the first block
     *  of the indicated list.
     */
    PageCursor(PageManager pageman, short type) throws IOException {
        this.pageman = pageman;
        this.type = type;
    }
    
    /**
     *  Returns the current value of the cursor.
     */
    long getCurrent() throws IOException {
        return current;
    }
    
    /**
     *  Returns the next value of the cursor
     */
    long next() throws IOException {
        if (current == 0)
            current = pageman.getFirst(type);
        else
            current = pageman.getNext(current);
        return current;
    } 
    
    /**
     *  Returns the previous value of the cursor
     */
    long prev() throws IOException {
        current = pageman.getPrev(current);
        return current;
    }
}
