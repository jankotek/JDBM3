package jdbm;

import java.io.IOException;

public class TestLargeData extends TestCaseWithTestFile{

	public void testLargeData() throws IOException{
	    		
		DBAbstract db = new DBStore(newTestFile(), false, false);

		byte[] data = UtilTT.makeRecord(1000000, (byte) 12);
		final long id = db.insert(data);
		data = (byte[]) db.fetch(id);
		UtilTT.checkRecord(data, 1000000, (byte) 12);
		db.commit();

		data = UtilTT.makeRecord(2000000, (byte) 13);
		db.update(id, data);
		db.commit();
		data = (byte[]) db.fetch(id);
		UtilTT.checkRecord(data, 2000000, (byte) 13);
		db.commit();

		data = UtilTT.makeRecord(1500000, (byte) 14);
		db.update(id, data);
		data = (byte[]) db.fetch(id);
		UtilTT.checkRecord(data, 1500000, (byte) 14);
		db.commit();
		
		data = UtilTT.makeRecord(2500000, (byte) 15);
		db.update(id, data);
		db.rollback();
		data = (byte[]) db.fetch(id);
		UtilTT.checkRecord(data, 1500000, (byte) 14);
		db.commit();

		data = UtilTT.makeRecord(1, (byte) 20);
		db.update(id, data);
		data = (byte[]) db.fetch(id);
		UtilTT.checkRecord(data, 1, (byte) 20);
		db.commit();
				
		
	}
	
}
