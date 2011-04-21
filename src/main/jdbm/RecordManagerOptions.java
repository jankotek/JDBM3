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

/**
 * Standard options for RecordManager.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @author <a href="cg@cdegroot.com">Cees de Groot</a>
 * @version $Id: RecordManagerOptions.java,v 1.1 2002/05/31 06:33:20 boisvert Exp $
 */
public class RecordManagerOptions
{

    /**
     * Class name of the provider factory. Defaults to {@link jdbm.recman.Provider}.
     */
    public static final String PROVIDER_FACTORY = "jdbm.provider";

    /**
     * Option to create a thread-safe record manager.
     */
    public static final String THREAD_SAFE = "jdbm.threadSafe";


    /**
     * Option to automatically commit data after each operation.
     */
    public static final String AUTO_COMMIT = "jdbm.autoCommit";


    /**
     * Option to disable transaction (to increase performance at the cost of
     * potential data loss).
     * <br>Possible values: <u>false</u>|true.
     */
    public static final String DISABLE_TRANSACTIONS = "jdbm.disableTransactions";


    /**
     * Option to disable free space reclaim. New records are always
     * inserted to end of file. Usefull mainly on batch imports.
     * You will need to defrag() store after you are done
     *
     * <br>Possible values: <u>false</u>|true.
     */
    public static final String APPEND_TO_END = "jdbm.appendToEnd";

    /**
     * Type of cache to use. 
     * <br>Possible values: <u>auto</u>|none|mru|soft.
     * <p> 
     * With auto soft cache is used if VM have more then 1000 MB available.
     * Bellow that mru cache is used.
     * 
     */
    public static final String CACHE_TYPE = "jdbm.cache.type";

    
    /**
     * Size of the MRU cache. This affects cache type "mru" and cache type "soft".
     * <br>Default value: <u>1000</u>.
     */
    public static final String CACHE_SIZE = "jdbm.cache.size";


    /**
     * Compress pages in RecordManager with ZLIB. 
     * This may result in better space usage, but lower performance.
     * <br>Possible values: <u>false</u>|true.
     */
	public static final String COMPRESS = "jdbm.compress";

}
