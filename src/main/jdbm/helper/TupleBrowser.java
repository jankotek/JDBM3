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

package jdbm.helper;

import java.io.IOException;

/**
 * Browser to traverse a collection of tuples.  The browser allows for
 * forward and reverse order traversal.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: TupleBrowser.java,v 1.2 2001/05/19 14:02:00 boisvert Exp $
 */
public interface  TupleBrowser<K,V> {

    /**
     * Get the next tuple.
     *
     * @param tuple Tuple into which values are copied.
     * @return True if values have been copied in tuple, or false if there is
     *         no next tuple.
     */
    public abstract boolean getNext( Tuple<K,V> tuple )
        throws IOException;


    /**
     * Get the previous tuple.
     *
     * @param tuple Tuple into which values are copied.
     * @return True if values have been copied in tuple, or false if there is
     *         no previous tuple.
     */
    public abstract boolean getPrevious( Tuple<K,V> tuple )
        throws IOException;

}
