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

import java.io.IOException;
import java.util.Properties;

/**
 *  Provider of RecordManager implementation.  Classes implementing this
 *  interface act as a factory to provide implementations of RecordManager.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: RecordManagerProvider.java,v 1.2 2005/06/25 23:12:31 doomdark Exp $
 */
public interface RecordManagerProvider
{

    /**
     * Create a record manager.
     *
     * @param filename Base filename of the record file.
     * @param options Record manager options.
     * @throws IOException if an I/O related exception occurs while creating
     *                    or opening the record manager.
     * @throws UnsupportedOperationException if some options are not supported by the
     *                                      implementation.
     * @throws IllegalArgumentException if some options are invalid.
     */
    public RecordManager createRecordManager( String filename,
                                              Properties options )
        throws IOException;
}
