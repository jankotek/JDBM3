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
 *  This interface contains magic cookies.
 */
interface Magic {
    /** Magic cookie at start of file */
    public short FILE_HEADER = 0x1350;

    /** Magic for blocks. They're offset by the block type magic codes. */
    public short BLOCK = 0x1351;

    /** Magics for blocks in certain lists. Offset by baseBlockMagic */
    short FREE_PAGE = 0;
    short USED_PAGE = 1;
    short TRANSLATION_PAGE = 2;
    short FREELOGIDS_PAGE = 3;
    short FREEPHYSIDS_PAGE = 4;

    /** Number of lists in a file */
    public short NLISTS = 5;

    /**
     *  Maximum number of blocks in a file, leaving room for a 16 bit
     *  offset encoded within a long.
     */
    long MAX_BLOCKS = 0x7FFFFFFFFFFFL;

    /** Magic for transaction file */
    short LOGFILE_HEADER = 0x1360;

    /** Size of an externalized byte */
    public short SZ_BYTE = 1;
    /** Size of an externalized short */
    public short SZ_SHORT = 2;
    /** Size of an externalized unsigned short */
    public short SZ_UNSIGNED_SHORT = 2;    
    /** Size of an externalized int */
    public short SZ_INT = 4;
    /** Size of an externalized long */
    public short SZ_LONG = 8;

    /** size of three byte integer */
	public short SZ_THREE_BYTE_INT = 3;
	
    /** size of three byte integer */
	public short SZ_SIX_BYTE_LONG = 6;

}
