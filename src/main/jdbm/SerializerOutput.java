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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import jdbm.helper.LongPacker;
import jdbm.helper.Serialization;


/**
 * 
 * Output for Serializer
 * 
 * @author Jan Kotek
 *
 */
public class SerializerOutput extends DataOutputStream{
	
	
	public SerializerOutput(OutputStream out) {
		super(out);
	}

	public void writeObject(Object obj) throws IOException{
		Serialization.writeObject(this, obj);
	}
	
	public void writePackedLong(long i) throws IOException{
		LongPacker.packLong(this, i);
	}

	public void writePackedInt(int i) throws IOException{
		LongPacker.packInt(this, i);
	}

    /**
     * Reset counter inside DataOutputStream.
     * Workaround method if SerializerOutput instance is reused
     *
     */
        public void __resetWrittenCounter(){
            written = 0;
        }

}
