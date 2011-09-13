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

import java.io.IOException;

public class StoreReferenceTest extends TestCaseWithTestFile{
	
	public void test() throws IOException{
		String file = newTestFile();
		RecordManager r = RecordManagerFactory.createRecordManager(file);
		PrimaryTreeMap<Long,StoreReference<String>> t = r.treeMap("aaa");
		
		t.put(1l, new StoreReference(r,"1"));
		t.put(2l, new StoreReference(r,"2"));
		r.commit();
		
		assertEquals("1",t.get(1l).get(r));
		assertEquals("2",t.get(2l).get(r));
		
		//reopen store
		r.close();
		r = RecordManagerFactory.createRecordManager(file);
		t = r.treeMap("aaa");
		assertEquals("1",t.get(1l).get(r));
		assertEquals("2",t.get(2l).get(r));
		r.close();
		
	}
}
