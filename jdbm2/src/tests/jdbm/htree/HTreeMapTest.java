package jdbm.htree;

import java.io.IOException;
import java.util.Map;

import jdbm.RecordManager;
import jdbm.recman._TestCaseWithTestFile;

public class HTreeMapTest extends MapInterfaceTest<Integer,String>{
	
	public HTreeMapTest(String name){
		super(name, false,false,true,true,true,true);
	}
	
	RecordManager r;
	
	public void setUp() throws Exception{
		r = _TestCaseWithTestFile.newRecordManager();		
	}

	@Override
	protected Integer getKeyNotInPopulatedMap() throws UnsupportedOperationException {
		return -100;
	}

	@Override
	protected String getValueNotInPopulatedMap() throws UnsupportedOperationException {
		return "XYZ";
	}

	@Override
	protected Map<Integer,String> makeEmptyMap() throws UnsupportedOperationException {
		try {
			HTree<Integer,String> b = HTree.createInstance(r);
			return new HTreeMap<Integer, String>(b,false);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} 		
	}

	@Override
	protected Map<Integer,String> makePopulatedMap() throws UnsupportedOperationException {
		Map<Integer,String> map = makeEmptyMap();
		for(int i =0;i<100;i++)
			map.put(i, "aa"+i);
		return map;
	}
	
}
