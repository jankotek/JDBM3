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

import jdbm.recman.BaseRecordManager;
import jdbm.recman.CacheRecordManager;

import java.io.IOException;
import java.util.Properties;

/**
 * This is the factory class to use for instantiating {@link RecordManager}
 * instances.
 * 
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @author <a href="cg@cdegroot.com">Cees de Groot</a>
 * @version $Id: RecordManagerFactory.java,v 1.2 2005/06/25 23:12:31 doomdark
 *          Exp $
 */
public final class RecordManagerFactory {

	/**
	 * Create a record manager.
	 * 
	 * @param name
	 *            Name of the record file.
	 * @throws IOException
	 *             if an I/O related exception occurs while creating or opening
	 *             the record manager.
	 * @throws UnsupportedOperationException
	 *             if some options are not supported by the implementation.
	 * @throws IllegalArgumentException
	 *             if some options are invalid.
	 */
	public static RecordManager createRecordManager(String name)
			throws IOException {
		return createRecordManager(name, new Properties());
	}

	/**
	 * Create a record manager.
	 * 
	 * @param name
	 *            Name of the record file.
	 * @param options
	 *            Record manager options.
	 * @throws IOException
	 *             if an I/O related exception occurs while creating or opening
	 *             the record manager.
	 * @throws UnsupportedOperationException
	 *             if some options are not supported by the implementation.
	 * @throws IllegalArgumentException
	 *             if some options are invalid.
	 */
	@SuppressWarnings("unchecked")
	public static RecordManager createRecordManager(String name,
			Properties options) throws IOException {

            RecordManager recman = new BaseRecordManager( name );

            String value = options.getProperty( RecordManagerOptions.DISABLE_TRANSACTIONS, "false" );
            if ( value.equalsIgnoreCase( "TRUE" ) ) {
                ( (BaseRecordManager) recman ).disableTransactions();
            }

            value = options.getProperty(RecordManagerOptions.COMPRESS,"false");
            boolean compress = value.equalsIgnoreCase("TRUE");
            if(compress)
                    ( (BaseRecordManager) recman ).setCompress(true);

            value = options.getProperty(RecordManagerOptions.APPEND_TO_END,"false");
            boolean append = value.equalsIgnoreCase("TRUE");
            if(append)
                    ( (BaseRecordManager) recman ).setAppendToEnd(true);


            String cacheType = options.getProperty( RecordManagerOptions.CACHE_TYPE, "auto" );

            value = options.getProperty( RecordManagerOptions.CACHE_SIZE, "1000" );
            int cacheSize = Integer.parseInt( value );

            if("auto".equals(cacheType)){
                    try{
                            //disable SOFT if available memory is bellow 50 MB
                            if(Runtime.getRuntime().maxMemory()<=50000000)
                                    cacheType = "mru";
                            else
                                    cacheType = "soft";
                    }catch(Exception e){
                            cacheType = "mru";
                    }
            }

            if ("mru".equals(cacheType)) {
                    if(cacheSize>0){
                            recman = new CacheRecordManager( recman,cacheSize,false);
                    }
            }else if ("soft".equals(cacheType)) {
                    // cachesize is the size of the internal MRU, not the soft cache
                    recman = new CacheRecordManager(recman, cacheSize,true);

            }else if ("none".equals(cacheType)) {
                    //do nothing
            }else{
                    throw new IllegalArgumentException("Unknown cache type: "+cacheType);
            }

            return recman;

	}

}
