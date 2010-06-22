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

package jdbm.htree;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import jdbm.Serializer;

/**
 *  Abstract class for Hashtable directory nodes
 *
 *  @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 *  @version $Id: HashNode.java,v 1.2 2003/03/21 02:54:58 boisvert Exp $
 */
@SuppressWarnings("unchecked")
class HashNode<K,V> //implements Serializable, Serializer<HashNode>
{
	
//	static final Serializer SERIALIZER = DefaultSerializer.INSTANCE;
	static final Serializer<HashNode> SERIALIZER = new Serializer<HashNode>(){
	
	public HashNode deserialize(DataInputStream ds) throws IOException {
		try{
			if(ds.readBoolean()){ //is HashBucket?
				HashBucket ret = new HashBucket();
				ret.readExternal(ds);
				if(ds.available()!=0 && ds.read()!=-1) // -1 is fix for compression, not sure what is happening
					throw new InternalError("bytes left: "+ds.available()+1);
				return ret;
			}else{
				HashDirectory ret = new HashDirectory();
				ret.readExternal(ds);
				if(ds.available()!=0 && ds.read()!=-1) // -1 is fix for compression, not sure what is happening
					throw new InternalError("bytes left: "+ds.available()+1);
				return ret;
			}
		}catch(ClassNotFoundException e){
			throw new IOException(e);
		}
		
	}
	public void serialize(DataOutputStream out, HashNode obj) throws IOException {
		if(obj.getClass() ==  HashBucket.class){
			out.writeBoolean(true);
			HashBucket b = (HashBucket) obj;
			b.writeExternal(out);
		}else{
			out.writeBoolean(false);
			HashDirectory n = (HashDirectory) obj;
			n.writeExternal(out);
		}
	}

	};

}
