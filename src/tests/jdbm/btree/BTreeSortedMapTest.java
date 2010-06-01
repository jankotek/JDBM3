package jdbm.btree;

import java.io.IOException;
import java.util.SortedMap;

import jdbm.RecordManager;
import jdbm.recman.TestCaseWithTestFile;

public class BTreeSortedMapTest extends SortedMapInterfaceTest<Integer,String>{
	
	public BTreeSortedMapTest(String name){
		super(name, false,false,true,true,true,true);
	}
	
	RecordManager r;
	
	public void setUp() throws Exception{
		r = TestCaseWithTestFile.newRecordManager();		
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
	protected SortedMap<Integer,String> makeEmptyMap() throws UnsupportedOperationException {
		try {
			BTree<Integer,String> b = BTree.createInstance(r);
			return new BTreeSortedMap<Integer, String>(b,false);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} 		
	}

	@Override
	protected SortedMap<Integer,String> makePopulatedMap() throws UnsupportedOperationException {
		SortedMap<Integer,String> map = makeEmptyMap();
		for(int i =0;i<100;i++)
			map.put(i, "aa"+i);
		return map;
	}
	
}
