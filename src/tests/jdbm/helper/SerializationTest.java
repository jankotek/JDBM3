/*******************************************************************************
 * Copyright 2010 Cees De Groot, Alex Boisvert, Jan Kotek
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package jdbm.helper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.AbstractMap.SimpleEntry;

import junit.framework.TestCase;

@SuppressWarnings("unchecked")
public class SerializationTest extends TestCase{

	public SerializationTest(String name) {
		super(name);
	}
	
	public void testInt() throws IOException, ClassNotFoundException{
		int[] vals = {
				Integer.MIN_VALUE,
				-Short.MIN_VALUE * 2,
				-Short.MIN_VALUE +1,
				-Short.MIN_VALUE ,
				-10,-9,-8,-7,-6,-5,-4,-1,0,1,2,3,4,5,6,7,8,9,10,
				127,254,255,256,Short.MAX_VALUE,Short.MAX_VALUE+1,
				Short.MAX_VALUE*2, Integer.MAX_VALUE				
		};
		for(int i :vals){
			byte[] buf = Serialization.serialize(i);
			Object l2 = Serialization.deserialize(buf);
			assertTrue(l2.getClass() == Integer.class);
			assertEquals(l2,i);
		}
	}

	public void testShort() throws IOException, ClassNotFoundException{
		short[] vals = {				
				(short) (-Short.MIN_VALUE +1),
				(short) -Short.MIN_VALUE ,
				-10,-9,-8,-7,-6,-5,-4,-1,0,1,2,3,4,5,6,7,8,9,10,
				127,254,255,256,Short.MAX_VALUE,Short.MAX_VALUE-1,
				Short.MAX_VALUE				
		};
		for(short i :vals){
			byte[] buf = Serialization.serialize(i);
			Object l2 = Serialization.deserialize(buf);
			assertTrue(l2.getClass() == Short.class);
			assertEquals(l2,i);
		}
	}
	
	public void testDouble() throws IOException, ClassNotFoundException{
		double[] vals = {			
				1f,0f,-1f,  Math.PI, 255,256,Short.MAX_VALUE, Short.MAX_VALUE+1, -100
		};
		for(double i :vals){
			byte[] buf = Serialization.serialize(i);
			Object l2 = Serialization.deserialize(buf);
			assertTrue(l2.getClass() == Double.class);
			assertEquals(l2,i);
		}
	}
		
	
	public void testFloat() throws IOException, ClassNotFoundException{
		float[] vals = {			
				1f,0f,-1f, (float) Math.PI, 255,256,Short.MAX_VALUE, Short.MAX_VALUE+1, -100
		};
		for(float i :vals){
			byte[] buf = Serialization.serialize(i);
			Object l2 = Serialization.deserialize(buf);
			assertTrue(l2.getClass() == Float.class);
			assertEquals(l2,i);
		}
	}
	
	public void testChar() throws IOException, ClassNotFoundException{
		char[] vals = {				
				'a',' '				
		};
		for(char i :vals){
			byte[] buf = Serialization.serialize(i);
			Object l2 = Serialization.deserialize(buf);
			assertTrue(l2.getClass() == Character.class);
			assertEquals(l2,i);
		}
	}
	
	
	public void testLong() throws IOException, ClassNotFoundException{
		long[] vals = {
				Long.MIN_VALUE,
				Integer.MIN_VALUE,Integer.MIN_VALUE-1,Integer.MIN_VALUE+1,
				-Short.MIN_VALUE * 2,
				-Short.MIN_VALUE +1,
				-Short.MIN_VALUE ,
				-10,-9,-8,-7,-6,-5,-4,-1,0,1,2,3,4,5,6,7,8,9,10,
				127,254,255,256,Short.MAX_VALUE,Short.MAX_VALUE+1,
				Short.MAX_VALUE*2, Integer.MAX_VALUE, Integer.MAX_VALUE+1, Long.MAX_VALUE				
		};
		for(long i :vals){
			byte[] buf = Serialization.serialize(i);
			Object l2 = Serialization.deserialize(buf);
			assertTrue(l2.getClass() == Long.class);
			assertEquals(l2,i);
		}
	}
	
	public void testBoolean1() throws IOException, ClassNotFoundException{		
		byte[] buf = Serialization.serialize(true);
		Object l2 =  Serialization.deserialize(buf);
		assertTrue(l2.getClass() == Boolean.class);
		assertEquals(l2,true);
		
		byte[] buf2 = Serialization.serialize(false);
		Object l22 =  Serialization.deserialize(buf2);
		assertTrue(l22.getClass() == Boolean.class);
		assertEquals(l22,false);		

	}

	public void testString() throws IOException, ClassNotFoundException{		
		byte[] buf = Serialization.serialize("Abcd");
		String l2 = (String) Serialization.deserialize(buf);
		assertEquals(l2,"Abcd");
	}
	
	public void testBigString() throws IOException, ClassNotFoundException{
		String bigString = "";
		for(int i = 0;i<1e4;i++)
			bigString +=i%10;
		byte[] buf = Serialization.serialize(bigString);
		String l2 = (String) Serialization.deserialize(buf);
		assertEquals(l2,bigString);		
	}
	
	
	public void testObject() throws ClassNotFoundException, IOException{
		SimpleEntry a = new SimpleEntry(1,"11");
		byte[] buf = Serialization.serialize(a);
		SimpleEntry l2 = (SimpleEntry) Serialization.deserialize(buf);
		assertEquals(l2,a);
	}
	
	public void testArrayList() throws ClassNotFoundException, IOException{
		Collection c = new ArrayList();
		for(int i = 0; i<200;i++)
			c.add(i);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));		
		for(int i = 0; i<2000;i++)
			c.add(i);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));
	}
	
	public void testLinkedList() throws ClassNotFoundException, IOException{
		Collection c = new LinkedList();
		for(int i = 0; i<200;i++)
			c.add(i);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));		
		for(int i = 0; i<2000;i++)
			c.add(i);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));
	}
	
	public void testVector() throws ClassNotFoundException, IOException{
		Collection c = new Vector();
		for(int i = 0; i<200;i++)
			c.add(i);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));		
		for(int i = 0; i<2000;i++)
			c.add(i);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));
	}
	
	
	public void testTreeSet() throws ClassNotFoundException, IOException{
		Collection c = new TreeSet();
		for(int i = 0; i<200;i++)
			c.add(i);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));		
		for(int i = 0; i<2000;i++)
			c.add(i);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));
	}

	public void testHashSet() throws ClassNotFoundException, IOException{
		Collection c = new HashSet();
		for(int i = 0; i<200;i++)
			c.add(i);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));		
		for(int i = 0; i<2000;i++)
			c.add(i);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));
	}

	public void testLinkedHashSet() throws ClassNotFoundException, IOException{
		Collection c = new LinkedHashSet();
		for(int i = 0; i<200;i++)
			c.add(i);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));		
		for(int i = 0; i<2000;i++)
			c.add(i);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));
	}

	public void testHashMap() throws ClassNotFoundException, IOException{
		Map c = new HashMap();
		for(int i = 0; i<200;i++)
			c.put(i,i+10000);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));		
		for(int i = 0; i<2000;i++)
			c.put(i,i+10000);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));
	}
	public void testTreeMap() throws ClassNotFoundException, IOException{
		Map c = new TreeMap();
		for(int i = 0; i<200;i++)
			c.put(i,i+10000);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));		
		for(int i = 0; i<2000;i++)
			c.put(i,i+10000);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));
	}
	
	public void testLinkedHashMap() throws ClassNotFoundException, IOException{
		Map c = new LinkedHashMap();
		for(int i = 0; i<200;i++)
			c.put(i,i+10000);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));		
		for(int i = 0; i<2000;i++)
			c.put(i,i+10000);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));
	}
	
	public void testHashtable() throws ClassNotFoundException, IOException{
		Map c = new Hashtable();
		for(int i = 0; i<200;i++)
			c.put(i,i+10000);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));		
		for(int i = 0; i<2000;i++)
			c.put(i,i+10000);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));
	}
	
	public void testProperties() throws ClassNotFoundException, IOException{
		Properties c = new Properties();
		for(int i = 0; i<200;i++)
			c.put(i,i+10000);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));		
		for(int i = 0; i<2000;i++)
			c.put(i,i+10000);
		assertEquals(c, Serialization.deserialize(Serialization.serialize(c)));
	}
	

	public void testClass() throws IOException, ClassNotFoundException{		
		byte[] buf = Serialization.serialize(String.class);
		Class l2 = (Class) Serialization.deserialize(buf);
		assertEquals(l2,String.class);
	}
	
	public void testClass2() throws IOException, ClassNotFoundException{		
		byte[] buf = Serialization.serialize(long[].class);
		Class l2 = (Class) Serialization.deserialize(buf);
		assertEquals(l2,long[].class);
	}	

	
	public void testUnicodeString() throws ClassNotFoundException, IOException{
		String s = "Ciudad Bolíva";
		byte[] buf = Serialization.serialize(s);
		assertTrue("text is not unicode",buf.length!=s.length());
		Object l2 =  Serialization.deserialize(buf);
		assertEquals(l2,s);		
	}
	
	public void testSerializationHeader() throws IOException{
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		new ObjectOutputStream(b).writeObject("lalala");
		ByteArrayInputStream i = new ByteArrayInputStream(b.toByteArray());
		final int header1 = i.read();

		ByteArrayOutputStream b2 = new ByteArrayOutputStream();
		new ObjectOutputStream(b2).writeObject(new Integer(1));
		ByteArrayInputStream i2 = new ByteArrayInputStream(b2.toByteArray());
		final int header2 = i2.read();

		assertEquals(header1, header2);
		assertEquals(header1, Serialization.JAVA_SERIALIZATION);
		System.out.println("serialization header: "+header1);
		
		
		
		
	}
}
