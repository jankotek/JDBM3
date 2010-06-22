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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOError;
import java.io.IOException;

import jdbm.RecordManager;
import jdbm.Serializer;

public class StoreReference<E> {

	private long recid = -1;

	/** this constructor is package private, and is used by Serialization */
	StoreReference(){
	}
	
	/** Public constructor which takes value and RecordManager */
	public StoreReference(RecordManager recman, E val,Serializer<E> serializer){
		if(recman == null)
			throw new IllegalArgumentException("RecordManager is null");
		if(serializer == null)
			throw new IllegalArgumentException("Serializer is null");
		
		if(val == null)
			throw new IllegalArgumentException("Can not store null value");
		if(val instanceof StoreReference)
			throw new IllegalArgumentException("Can not store other reference");
		
		try {
			recid = recman.insert(val, serializer);
		} catch (IOException e) {
			throw new IOError(e);
		}
	}
	
	public StoreReference(RecordManager recman, E val){
		this(recman,val,(Serializer<E>) DefaultSerializer.INSTANCE);
	}
	
	
	public E get(RecordManager recman2, Serializer<E> serializer2){
//		if(val!=null)
//			return val;
		if(recid!=-1)try{			
			return recman2.fetch(recid,serializer2);
		}catch(IOException e){
			throw new IOError(e);
		}
		throw new IllegalStateException("Should not be here");			
	}
	
	public E get(RecordManager recman2){
		return get(recman2,(Serializer<E>) DefaultSerializer.INSTANCE);
	}
	
	void writeExternal(DataOutputStream d) throws IOException{
		if(recid==-1)
			throw new IllegalStateException("not initialized");
			
		
		d.writeLong(recid);
	}
	
	void readExternal(DataInputStream d) throws IOException{
		if(recid!=-1)
			throw new IllegalStateException("already initialized");
		recid = d.readLong();
	}

	public long getRecId() {
		return recid;
	}

	public void remove(RecordManager recordManager) {
		if(recid==-1)
			throw new IllegalStateException("alread removed");

		try {
			recordManager.delete(recid);
		} catch (IOException e) {
			throw new IOError(e);
		}
	}
	
}
