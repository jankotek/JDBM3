package jdbm.recman;

import java.io.IOException;

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
}
