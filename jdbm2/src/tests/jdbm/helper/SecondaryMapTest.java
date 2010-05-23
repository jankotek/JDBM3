package jdbm.helper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.SecondaryHashMap;
import jdbm.SecondaryKeyExtractor;
import jdbm.SecondaryTreeMap;
import jdbm.recman._TestCaseWithTestFile;

public class SecondaryMapTest extends _TestCaseWithTestFile{

	<E> List<E> list(E... obj){
		List<E> l = new ArrayList<E>();
		for(E o:obj) l.add(o);
		return l;
	}
	
	
	public void testSecondaryTreeMap() throws IOException{
		RecordManager r = newRecordManager();
		PrimaryTreeMap<Integer, String> m = r.treeMap("aa");
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
		PrimaryTreeMap<Integer, String> m = r.treeMap("aa");
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
		PrimaryTreeMap<Integer, String> m = r.treeMap("aa");
		SecondaryTreeMap<Integer, Integer, String> s = m.secondaryTreeMapManyToOne("bb", 
				new SecondaryKeyExtractor<List<Integer>, Integer, String>() {

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
		PrimaryTreeMap<Integer, String> m = r.treeMap("aa");
		SecondaryHashMap<Integer, Integer, String> s = m.secondaryHashMapManyToOne("bb", 
				new SecondaryKeyExtractor<List<Integer>, Integer, String>() {

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

	
}
