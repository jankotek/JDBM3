///*******************************************************************************
// * Copyright 2010 Cees De Groot, Alex Boisvert, Jan Kotek
// * 
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// * 
// *   http://www.apache.org/licenses/LICENSE-2.0
// * 
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// ******************************************************************************/
//
//
//package jdbm.recman;
//
//import java.io.IOException;
//
///**
// *  This interface is used for synchronization.
// *  <p>
// *  RecordManager ensures that the cache has the up-to-date information
// *  by way of an invalidation protocol.
// */
//public interface RecordCache {
//
//    /**
//     * Notification to flush content related to a given record.
//     */
//    public void flush(long recid) throws IOException;
//
//    /**
//     * Notification to flush data all of records.
//     */
//    public void flushAll() throws IOException;
//
//    /**
//     * Notification to invalidate content related to given record.
//     */
//    public void invalidate(long recid) throws IOException;
//
//    /**
//     * Notification to invalidate content of all records.
//     */
//    public void invalidateAll() throws IOException;
//
//}
