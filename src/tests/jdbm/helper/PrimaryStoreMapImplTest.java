package jdbm.helper;

import java.util.Map;

import jdbm.PrimaryStoreMap;
import jdbm.RecordManager;
import jdbm.htree.MapInterfaceTest;
import jdbm.recman.TestCaseWithTestFile;

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
