/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 */

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
@SuppressWarnings("unchecked")class HashNode<K,V> //implements Serializable, Serializer<HashNode>{	//	static final Serializer SERIALIZER = DefaultSerializer.INSTANCE;	static final Serializer<HashNode> SERIALIZER = new Serializer<HashNode>(){	
	public HashNode deserialize(DataInputStream ds) throws IOException {		try{			if(ds.readBoolean()){ //is HashBucket?				HashBucket ret = new HashBucket();				ret.readExternal(ds);				if(ds.available()!=0 && ds.read()!=-1) // -1 is fix for compression, not sure what is happening					throw new InternalError("bytes left: "+ds.available()+1);				return ret;			}else{				HashDirectory ret = new HashDirectory();				ret.readExternal(ds);				if(ds.available()!=0 && ds.read()!=-1) // -1 is fix for compression, not sure what is happening					throw new InternalError("bytes left: "+ds.available()+1);				return ret;			}		}catch(ClassNotFoundException e){			throw new IOException(e);		}			}	public void serialize(DataOutputStream out, HashNode obj) throws IOException {		if(obj.getClass() ==  HashBucket.class){			out.writeBoolean(true);			HashBucket b = (HashBucket) obj;			b.writeExternal(out);		}else{			out.writeBoolean(false);			HashDirectory n = (HashDirectory) obj;			n.writeExternal(out);		}	}
	};

}
