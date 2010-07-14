package jdbm.recman;

import java.io.IOException;
import java.util.TreeMap;

import jdbm.PrimaryTreeMap;

public class TestDefrag extends TestCaseWithTestFile{

	
	public void testDefrag1() throws IOException{
		String file = newTestFile();
		BaseRecordManager m = new BaseRecordManager(file);
		long loc = m.insert("123");
		m.defrag();		
		m.close();
		m = new BaseRecordManager(file);
		assertEquals(m.fetch(loc),"123");
	}
	

	
	public void testDefragBtree() throws IOException{
		String file = newTestFile();
		BaseRecordManager m = new BaseRecordManager(file);
		PrimaryTreeMap t = m.treeMap("aa");
		TreeMap t2 = new TreeMap();
		for(int i =0;i<10000;i ++ ){
			t.put(i, ""+i);
			t2.put(i, ""+i);
		}
					
		m.defrag();		
		m.close();
		m = new BaseRecordManager(file);
		t = m.treeMap("aa");
		assertEquals(t,t2);
	}
}
