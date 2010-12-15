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

import java.io.IOException;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;

public class TestFileLock extends TestCaseWithTestFile {

	public void testLock() throws IOException{
		String file = newTestFile();
		
		RecordManager recman1 = RecordManagerFactory.createRecordManager(file);		
		//now open same file second time, exception should be thrown
		try{
			RecordManager recman2 = RecordManagerFactory.createRecordManager(file);
			fail("Exception should be thrown if file was locked");
		}catch(IOException e){
			//expected
		}
				
		recman1.close();
		
		//after close lock should be released, reopen
		RecordManager recman3 = RecordManagerFactory.createRecordManager(file);
		recman3.close();
	}
}
