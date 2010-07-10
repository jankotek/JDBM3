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
 * This class represents a location within a file. Both physical and
 * logical rowids are based on locations internally - this version is
 * used when there is no file block to back the location's data.
 */
final class Location {
	
	static long getBlock(long blockOffset){
		return blockOffset >> 16;
	}
	
	static short getOffset(long blockOffset){
		return (short) (blockOffset & 0xffff);
	}
	
	static long toLong(long block, short offset){
		return (block << 16) + (long) offset;
	}

	
}
