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


import java.io.IOException;

import jdbm.Serializer;
import jdbm.SerializerInput;
import jdbm.SerializerOutput;

/**
 * Default java serializer. Constructor is privite, use  DefaultSerializer.INSTANCE
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 */
public class DefaultSerializer
    implements Serializer<Object>
{

	private static final long serialVersionUID = -3818545055661017388L;
	
	public static final DefaultSerializer INSTANCE = new DefaultSerializer();
    
    
    /**
     * Construct a DefaultSerializer, is private to make sure every one uses INSTANCE
     */
    private DefaultSerializer()
    {
        // no op
    }

    
    /**
     * Serialize the content of an object into a byte array.
     *
     * @param obj Object to serialize
     * @return a byte array representing the object's state
     */
     public void serialize(SerializerOutput out,Object obj)
        throws IOException
     {    
    	 out.writeObject(obj);        
     }
        
        
    /**
     * Deserialize the content of an object from a byte array.
     *
     * @param serialized Byte array representation of the object
     * @return deserialized object
     */
     public Object deserialize(SerializerInput in )
        throws IOException
     {
         try {
            return in.readObject();
         } catch ( ClassNotFoundException except ) {
            throw new IOException( except );
         }
     }

}
