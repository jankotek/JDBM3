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

import java.util.Map;

public class PrimaryStoreMapImplTest extends MapInterfaceTest<Long,String>{
	
	public PrimaryStoreMapImplTest(String name){
		super(name, false,false,false,true,true,true);
	}
	
	RecordManager r;
	
	public void setUp() throws Exception{
		r = TestCaseWithTestFile.newRecordManager();		
	}

	@Override
	protected Long getKeyNotInPopulatedMap() throws UnsupportedOperationException {
		return -100l;
	}

	@Override
	protected String getValueNotInPopulatedMap() throws UnsupportedOperationException {
		return "XYZ";
	}

	@Override
	protected Map<Long,String> makeEmptyMap() throws UnsupportedOperationException {
		return r.storeMap("storeMap");
	}

	@Override
	protected Map<Long,String> makePopulatedMap() throws UnsupportedOperationException {
		PrimaryStoreMap<Long, String> map = r.storeMap("storeMap");
		for(int i =0;i<100;i++)
			map.putValue("aa"+i);
		return map;
	}

	public void testPutExistingKey() {}
	public void testPutAllExistingKey() {}
}
