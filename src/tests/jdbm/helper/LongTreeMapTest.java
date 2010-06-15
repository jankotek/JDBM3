package jdbm.helper;

import java.util.Iterator;
import java.util.Random;
import java.util.TreeMap;

import junit.framework.TestCase;

public class LongTreeMapTest extends TestCase{

	public void testAll(){
		LongTreeMap<String> t = new LongTreeMap<String>();
		t.put(1, "aa");
		t.put(2, "bb");
		t.put(2, "bb");		
		t.put(4, "cc");
		t.put(9, "FF");
		assertEquals(4, t.size());
		t.remove(1);
		assertEquals(3, t.size());
		assertEquals(t.get(1),null);
		assertEquals(t.get(2),"bb");
		assertEquals(t.get(3),null);
		assertEquals(t.get(4),"cc");
		assertEquals(t.get(5),null);
		assertEquals(t.get(-1),null);
		assertEquals(t.get(9),"FF");
		
		Iterator<String> vals = t.valuesIterator();
		assertTrue(vals.hasNext());
		assertEquals(vals.next(), "bb");
		assertTrue(vals.hasNext());
		assertEquals(vals.next(), "cc");
		assertTrue(vals.hasNext());
		assertEquals(vals.next(), "FF");
		
		assertFalse(vals.hasNext());
		
		t.clear();
		assertEquals(0, t.size());
		t.put(2, "bb");
		assertEquals(1,t.size());
		assertEquals(t.get(1),null);
		assertEquals(t.get(2),"bb");
		assertEquals(t.get(3),null);

	}
	
	public void testRandomCompare(){
		LongTreeMap<String> v1 = new LongTreeMap<String>();
		TreeMap<Long,String> v2 = new TreeMap<Long,String>();	
		Random d = new Random();
		for(int i = 0; i<1000;i++){		
			long key = d.nextInt() %100;
			double random = d.nextDouble();
			if(random<0.8){
//				System.out.println("put "+key);
				v1.put(key, ""+key);
				v2.put(key,""+key);
			}else{
//				System.out.println("remove "+key);
				v1.remove(key);
				v2.remove(key);
			}
			checkEquals(v1, v2);
			
		}
	}
	
	public void checkEquals(LongTreeMap<String> v1 , TreeMap<Long,String> v2){
		assertEquals(v1.size(), v2.size());
		for(long k : v2.keySet()){
			assertEquals(v1.get(k), v2.get(k));
		}
		
		int counter = 0;
		Iterator<String> it = v1.valuesIterator();
		while(it.hasNext()){
			String v = it.next();
			long key = Long.valueOf(v);
			assertEquals(v1.get(key),v);
			assertEquals(""+key,v);
			counter++;
		}
		assertEquals(counter,v2.size());
	}
	
	
	public void test2(){
		LongTreeMap<String> v1 = new LongTreeMap<String>();
		v1.put(1611,"1611");		
		v1.put(15500,"15500");
		v1.put(9446,"9446");
		System.out.println(v1.get(9446));
		System.out.println(v1.toString());
		assertEquals(3,v1.size());
		assertEquals(v1.get(9446),"9446");

	}
	
	public void testMemoryConsuptio(){
		System.out.println("Memory available: "+(Runtime.getRuntime().maxMemory()/1e6)+"MB");
		long counter =0;
		LongKeyChainedHashMap<String> e = new LongKeyChainedHashMap<String>();
		//LongTreeMap<String> e = new LongTreeMap<String>();
		while(true){
			counter++;
			e.put(counter, "");
			if(counter%1e6 == 0){
				System.out.println(counter);
				System.out.println("Memory used: "+((Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())/1e6)+"MB");
			}
		}
			
	}
	
}
