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

import java.io.IOError;
import java.io.IOException;

/**
 *
 */
public class RecordManagerBuilder {

    private String cacheType = "auto";
    private int mruCacheSize = 512;

    private String location = null;

    private boolean batchInsert = false;
    private boolean disableTransactions = false;
    private boolean readonly = false;


    /**
     * Creates new RecordManagerBuilder and sets location where database is located.
     *
     * @param location on disk where db is located
     */
    public RecordManagerBuilder(String location){
        this.location = location;
    }


    /**
     * Use WeakReference for cache.
     * This cache does not improve performance much,
     * but prevents JDBM from creating multiple instances of the same object.
     *
     * @return this builder
     */
    public RecordManagerBuilder enableWeakCache(){
        cacheType = "weak";
        return this;
    }

    /**
     * Use SoftReference for cache.
     * This cache greatly improves performance if you have enoguth memory.
     * Instances in cache are Garbage Collected when memory gets low
     *
     * @return this builder
     */
    public RecordManagerBuilder enableSoftCache(){
        cacheType = "soft";
        return this;
    }

    /**
     * Use 'Most Recently Used' cache with limited size.
     * Oldest instances are released from cache when new instances are fetched.
     * This cache is not cleared by GC. Is good for systems with limited memory.
     * <p/>
     * Default size for MRU cache is 512 records.
     *
     * @return this builder
     */
    public RecordManagerBuilder enableMRUCache(){
        cacheType = "mru";
        return this;
    }

    /**
     * Activates 'Most Recently Used' cache and sets its size.
     *
     * @param cacheSize number of instances which will be kept in cache. Recommended size is 512
     * @return this builder
     */
    public RecordManagerBuilder enableMRUCache(int cacheSize){
        if(cacheSize<0) throw new IllegalArgumentException("Cache size is smaller than zero");
        cacheType = "mru";
        this.mruCacheSize = cacheSize;
        return this;
    }


    /**
     * Set cache type automatically depending on availably memory.
     * If availably memory size is bellow 50 MB, 'Most Recently Used' cache with 512 records will be used.
     * Otherwise SoftReference cache will be used.
     * <p/>
     * This is default cache setting.
     *
     * @return this builder
     */
    public RecordManagerBuilder enableAutoCache(){
        cacheType = "auto";
        return this;
    }


    /**
     * Make RecordManager readonly.
     * Update/delete/insert operation will throw 'UnsupportedOperationException'
     *
     * @return this builder
     */
    public RecordManagerBuilder readonly(){
        readonly = true;
        return this;
    }


    /**
     * Disable cache completely
     *
     * @return this builder
     */
    public RecordManagerBuilder disableCache(){
        cacheType = "none";
        return this;
    }


    /**
     * Enables  Batch Insert Mode. If this mode is activated,
     * JDBM can handle huge inserts and updates faster.
     * This is generally usefull when DB is populated with data at creation.
     * <p/>
     * In this mode JDBM disables a space-saving optimisations to make inserts faster.
     * After batch insert you should run 'RecordStore.defrag()' to optimise and defragment
     * file storage.
     * <p/>
     * Batch Insert Mode is disabled by default
     *
     * @return this builder
     */
    public RecordManagerBuilder enableBatchInsertMode(){
        this.batchInsert = true;
        return this;
    }

    /**
     * Option to disable transaction (to increase performance at the cost of potential data loss).
     * <p/>
     * Transactions are enabled by default
     *
     * @return this builder
     */
    public RecordManagerBuilder disableTransactions(){
        this.disableTransactions = false;
        return this;
    }


    /**
     * Opens database with settings earlier specified in this builder.
     *
     * @return new RecordManager
     * @throws java.io.IOError if db could not be opened
     */
    public RecordManager build(){
        RecordManager recman = null;

        try{
            recman = new RecordManagerStorage(location);
        }catch(IOException e){
            throw new IOError(e);
        }

        if(disableTransactions)
            ( (RecordManagerStorage) recman ).disableTransactions();

        if(batchInsert)
            ( (RecordManagerStorage) recman ).setAppendToEnd(true);

        if(readonly)
            ( (RecordManagerStorage) recman ).setReadonly(true);


        String cacheType2 = cacheType;
        if("auto".equals(cacheType)){
            try{
                    //disable SOFT if available memory is bellow 50 MB
                    if(Runtime.getRuntime().maxMemory()<=1024*1024*50)
                            cacheType2 = "mru";
                    else
                            cacheType2 = "soft";
            }catch(Exception e){
                    cacheType2 = "mru";
            }
        }

        if("mru".equals(cacheType2)){
            recman = new RecordManagerCache( recman,mruCacheSize,false,true);
        }else if("soft".equals(cacheType2)){
             recman = new RecordManagerCache(recman, 0,true,true);
        }else if("weak".equals(cacheType2)){
             recman = new RecordManagerCache(recman, 0,true,false);

        }else if("none".equals(cacheType2)){
            //do nothing
        }else{
            throw new IllegalArgumentException("Unknown cache type: "+cacheType2);
        }


         return recman;
    }


}
