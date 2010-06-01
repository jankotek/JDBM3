package jdbm.helper;

import java.io.IOException;

import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.recman.TestCaseWithTestFile;

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
