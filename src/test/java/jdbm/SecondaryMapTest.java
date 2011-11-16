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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class SecondaryMapTest extends TestCaseWithTestFile{

	<E> List<E> list(E... obj){
		List<E> l = new ArrayList<E>();
		for(E o:obj) l.add(o);
		return l;
	}
	
	
	public void testSecondaryTreeMap() throws IOException{
		RecordManager r = newRecordManager();
		PrimaryTreeMap<Integer, String> m = r.createTreeMap("aa");
		SecondaryTreeMap<Integer, Integer, String> s = m.secondaryTreeMap("bb", 
				new SecondaryKeyExtractor<Integer, Integer, String>() {

			public Integer extractSecondaryKey(Integer key, String value) {				
				return value.length();
			}
		});
		
		m.put(1, "a");
		m.put(2, "ab");
		m.put(3, "abc");
		
		assertEquals(s.get(1), list(1));
		assertEquals(s.get(2), list(2));
		assertEquals(s.get(3), list(3));
		m.remove(2);
		assertEquals(s.get(2), null);

	}
	
	public void testSecondaryHashMap() throws IOException{
		RecordManager r = newRecordManager();
		PrimaryTreeMap<Integer, String> m = r.createTreeMap("aa");
		SecondaryHashMap<Integer, Integer, String> s = m.secondaryHashMap("bb", 
				new SecondaryKeyExtractor<Integer, Integer, String>() {

			public Integer extractSecondaryKey(Integer key, String value) {				
				return value.length();
			}
		});
		
		m.put(1, "a");
		m.put(2, "ab");
		m.put(3, "abc");
		
		assertEquals(s.get(1), list(1));
		assertEquals(s.get(2), list(2));
		assertEquals(s.get(3), list(3));
		m.remove(2);
		assertEquals(s.get(2), null);

	}
	
	public void testSecondaryTreeMapManyToOne() throws IOException{
		RecordManager r = newRecordManager();
		PrimaryTreeMap<Integer, String> m = r.createTreeMap("aa");
		SecondaryTreeMap<Integer, Integer, String> s = m.secondaryTreeMapManyToOne("bb", 
				new SecondaryKeyExtractor<Iterable<Integer>, Integer, String>() {

			public List<Integer> extractSecondaryKey(Integer key, String value) {				
				return list(value.length(),10+value.length());
			}
		});
		
		m.put(1, "a");
		m.put(2, "ab");
		m.put(3, "abc");
		
		assertEquals(s.get(1), list(1));
		assertEquals(s.get(2), list(2));
		assertEquals(s.get(3), list(3));
		assertEquals(s.get(11), list(1));
		assertEquals(s.get(12), list(2));
		assertEquals(s.get(13), list(3));
		
		m.remove(2);
		assertEquals(s.get(2), null);
		assertEquals(s.get(12), null);
		
	}


	public void testSecondaryHashMapManyToOne() throws IOException{
		RecordManager r = newRecordManager();
		PrimaryTreeMap<Integer, String> m = r.createTreeMap("aa");
		SecondaryHashMap<Integer, Integer, String> s = m.secondaryHashMapManyToOne("bb", 
				new SecondaryKeyExtractor<Iterable<Integer>, Integer, String>() {

			public List<Integer> extractSecondaryKey(Integer key, String value) {				
				return list(value.length(),10+value.length());
			}
		});
		
		m.put(1, "a");
		m.put(2, "ab");
		m.put(3, "abc");
		
		assertEquals(s.get(1), list(1));
		assertEquals(s.get(2), list(2));
		assertEquals(s.get(3), list(3));
		assertEquals(s.get(11), list(1));
		assertEquals(s.get(12), list(2));
		assertEquals(s.get(13), list(3));
		
		m.remove(2);
		assertEquals(s.get(2), null);
		assertEquals(s.get(12), null);
		
	}
	
	public void testInverseHashView() throws IOException{
		RecordManager r = newRecordManager();
		PrimaryTreeMap<Integer, String> m = r.createTreeMap("aa");
		InverseHashView<Integer, String> inverse = m.inverseHashView("aaInverse");		
		m.put(1, "a");
		m.put(2, "ab");
		m.put(3, "abc");
		
		assertEquals(inverse.findKeyForValue("a"),new Integer(1));
		assertEquals(inverse.findKeyForValue("ab"),new Integer(2));
		assertEquals(inverse.findKeyForValue("abc"),new Integer(3));
		assertEquals(inverse.findKeyForValue("nonexist"),null);
	}

	static final class NoHashObject implements Serializable{}
	
	public void testInverseHashIdentityCheck() throws IOException{
		RecordManager r = newRecordManager();
		PrimaryTreeMap<Integer, Object> m = r.createTreeMap("aa");
		InverseHashView<Integer, Object> inverse = m.inverseHashView("aaInverse");
		try{
			for(int i =0; i<1000;i++){
				m.put(i, new NoHashObject());
			}
			fail("value does not implement HashCode properly, IllegalArgumentException should be thrown");
		}catch(IllegalArgumentException ignore){}
	}
	
}
