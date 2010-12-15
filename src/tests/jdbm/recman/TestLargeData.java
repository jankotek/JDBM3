package jdbm.recman;

import java.io.IOException;

import jdbm.RecordManager;

public class TestLargeData extends TestCaseWithTestFile{

	public void testLargeData() throws IOException{
	    		
		RecordManager recman = new BaseRecordManager(newTestFile());
		
		byte[] data = UtilTT.makeRecord(1000000, (byte) 12);
		final long id = recman.insert(data);		
		data = (byte[]) recman.fetch(id);
		UtilTT.checkRecord(data, 1000000, (byte) 12);
		recman.commit();

		data = UtilTT.makeRecord(2000000, (byte) 13);
		recman.update(id, data);
		recman.commit();
		data = (byte[]) recman.fetch(id);
		UtilTT.checkRecord(data, 2000000, (byte) 13);
		recman.commit();

		data = UtilTT.makeRecord(1500000, (byte) 14);
		recman.update(id, data);		
		data = (byte[]) recman.fetch(id);
		UtilTT.checkRecord(data, 1500000, (byte) 14);
		recman.commit();
		
		data = UtilTT.makeRecord(2500000, (byte) 15);
		recman.update(id, data);	
		recman.rollback();
		data = (byte[]) recman.fetch(id);
		UtilTT.checkRecord(data, 1500000, (byte) 14);
		recman.commit();

		data = UtilTT.makeRecord(1, (byte) 20);
		recman.update(id, data);		
		data = (byte[]) recman.fetch(id);
		UtilTT.checkRecord(data, 1, (byte) 20);
		recman.commit();
				
		
	}
	
}
