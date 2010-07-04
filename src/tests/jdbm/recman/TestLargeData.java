package jdbm.recman;

import java.io.IOException;

import jdbm.RecordManager;

public class TestLargeData extends TestCaseWithTestFile{

	public void testLargeData() throws IOException{
	    		
		RecordManager recman = new BaseRecordManager(newTestFile());
		
		byte[] data = TestUtil.makeRecord(1000000, (byte)12);		
		final long id = recman.insert(data);		
		data = (byte[]) recman.fetch(id);
		TestUtil.checkRecord(data, 1000000, (byte)12);
		recman.commit();

		data = TestUtil.makeRecord(2000000, (byte)13);		
		recman.update(id, data);
		recman.commit();
		data = (byte[]) recman.fetch(id);
		TestUtil.checkRecord(data, 2000000, (byte)13);
		recman.commit();

		data = TestUtil.makeRecord(1500000, (byte)14);		
		recman.update(id, data);		
		data = (byte[]) recman.fetch(id);
		TestUtil.checkRecord(data, 1500000, (byte)14);
		recman.commit();
		
		data = TestUtil.makeRecord(2500000, (byte)15);		
		recman.update(id, data);	
		recman.rollback();
		data = (byte[]) recman.fetch(id);
		TestUtil.checkRecord(data, 1500000, (byte)14);
		recman.commit();

		data = TestUtil.makeRecord(1, (byte)20);		
		recman.update(id, data);		
		data = (byte[]) recman.fetch(id);
		TestUtil.checkRecord(data, 1, (byte)20);
		recman.commit();
				
		
	}
	
}
