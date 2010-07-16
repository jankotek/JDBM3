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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import jdbm.recman.BlockIo;

/**
 * Serialization util. It reduces serialized data size for most common java types. 
 * <p>
 * Common pattern is one byte header which identifies data type, then size is written (if required) and 
 * data. 
 * <p>
 * On unknown types normal java serialization is used
 * 
 * <p>
 * Header byte values bellow 180 are reserved by author for future use. If you want to customize
 * this class, use values over 180, to be compatible with future updates.
 * 
 * 
 * @author Jan Kotek
 */
@SuppressWarnings("unchecked")
public final class Serialization
{
	
	public static final byte END_OF_NORMAL_SERIALIZATION = 111; 
	
	/** print statistics to STDOUT */
	public static final boolean DEBUG = false;
	
	/** if set to true, debug informations will be saved to store to make it more robust */
	public static final boolean DEBUGSTORE = false;
	
	private static final int DEBUGSTORE_DUMMY_START = 456456567;
	private static final int DEBUGSTORE_DUMMY_END = 1234456;
	
	public final static int NULL 			=   0;
	public final static int NORMAL 			=   1;
	public final static int BOOLEAN_TRUE 	=   2;
	public final static int BOOLEAN_FALSE 	=   3;
	public final static int INTEGER_MINUS_1 =   4;
	public final static int INTEGER_0 		=   5;
	public final static int INTEGER_1 		=   6;
	public final static int INTEGER_2 		=   7;
	public final static int INTEGER_3 		=   8;
	public final static int INTEGER_4 		=   9;
	public final static int INTEGER_5 		=  10;
	public final static int INTEGER_6 		=  11;
	public final static int INTEGER_7 		=  12;
	public final static int INTEGER_8 		=  13;
	public final static int INTEGER_255		=  14;	
	public final static int INTEGER_PACK_NEG=  15;
	public final static int INTEGER_PACK 	=  16;
	public final static int LONG_MINUS_1 	=  17;
	public final static int LONG_0 			=  18;
	public final static int LONG_1 			=  19;
	public final static int LONG_2 			=  20;
	public final static int LONG_3 			=  21;
	public final static int LONG_4 			=  22;
	public final static int LONG_5 			=  23;
	public final static int LONG_6 			=  24;
	public final static int LONG_7 			=  25;
	public final static int LONG_8 			=  26;
	public final static int LONG_PACK_NEG	=  27;
	public final static int LONG_PACK 		=  28;
	public final static int LONG_255 		=  29;
	public final static int LONG_MINUS_MAX	=  30;
	public final static int SHORT_MINUS_1 	=  31;
	public final static int SHORT_0 		=  32;
	public final static int SHORT_1 		=  33;
	public final static int SHORT_255 		=  34;
	public final static int SHORT_FULL		=  35;	
	public final static int BYTE_MINUS_1 	=  36;
	public final static int BYTE_0 			=  37;
	public final static int BYTE_1 			=  38;
	public final static int BYTE_FULL		=  39;	
	public final static int CHAR			=  40;
	public final static int FLOAT_MINUS_1 	=  41;
	public final static int FLOAT_0 		=  42;
	public final static int FLOAT_1 		=  43;
	public final static int FLOAT_255		=  44;
	public final static int FLOAT_SHORT		=  45;		
	public final static int FLOAT_FULL		=  46;	
	public final static int DOUBLE_MINUS_1 	=  47;
	public final static int DOUBLE_0 		=  48;
	public final static int DOUBLE_1 		=  49;
	public final static int DOUBLE_255		=  50;
	public final static int DOUBLE_SHORT	=  51;	
	public final static int DOUBLE_FULL		=  52;
	//TODO serialization for bigdecimal and biginteger
	public final static int BIGDECIMAL_255	=  53;
	public final static int BIGDECIMAL		=  54;
	public final static int BIGINTEGER_255	=  55;
	public final static int BIGINTEGER		=  56;
	public final static int INTEGER_MINUS_MAX=  57;	
 	

	
	public final static int ARRAY_INT_B_255		=  60;
	public final static int ARRAY_INT_B_INT		=  61;
	public final static int ARRAY_INT_S			=  62;
	public final static int ARRAY_INT_I			=  63;
	public final static int ARRAY_INT_PACKED	=  64;
	
	public final static int ARRAY_LONG_B		=  65;
	public final static int ARRAY_LONG_S		=  66;
	public final static int ARRAY_LONG_I		=  67;
	public final static int ARRAY_LONG_L		=  68;
	public final static int ARRAY_LONG_PACKED	=  69;

	public final static int ARRAY_BYTE_255		=  70;
	public final static int ARRAY_BYTE_INT		=  71;
	
	public final static int ARRAY_OBJECT_255	=  72;
	public final static int ARRAY_OBJECT		=  73;
	//special cases for BTree values which stores references
	public final static int ARRAY_OBJECT_PACKED_LONG =  74;
	public final static int ARRAYLIST_PACKED_LONG =  75;
	
	public final static int STRING_EMPTY		= 101;
	public final static int STRING_255			= 102;
	public final static int STRING				= 103;
	public final static int ARRAYLIST_255		= 104;
	public final static int ARRAYLIST			= 105;
	
	public final static int TREEMAP_255			= 106;
	public final static int TREEMAP				= 107;
	public final static int HASHMAP_255			= 108;
	public final static int HASHMAP				= 109;
	public final static int LINKEDHASHMAP_255	= 110;
	public final static int LINKEDHASHMAP		= 111;
	
	public final static int TREESET_255			= 112;
	public final static int TREESET				= 113;
	public final static int HASHSET_255			= 114;
	public final static int HASHSET				= 115;
	public final static int LINKEDHASHSET_255	= 116;
	public final static int LINKEDHASHSET		= 117;
	public final static int LINKEDLIST_255		= 118;
	public final static int LINKEDLIST			= 119;
	

	public final static int VECTOR_255			= 120;
	public final static int VECTOR				= 121;
	public final static int HASHTABLE_255		= 122;
	public final static int HASHTABLE			= 123;
	public final static int PROPERTIES_255		= 124;
	public final static int PROPERTIES			= 125;
	
	public final static int CLASS				= 126;	
	
	public final static int STOREREFERENCE		= 160;
	public final static int BLOCKIO				= 161;

	public static final int BPAGE_LEAF 			= 162;
	public static final int BPAGE_NONLEAF 		= 163;
	public static final int HTREE_BUCKET 		= 164;
	public static final int HTREE_DIRECTORY 	= 165;
	public static final int JAVA_SERIALIZATION 	= 172;
	
	
    /**
     * Serialize the object into a byte array.
     */
    public static byte[] serialize( Object obj )
        throws IOException
    {
    	ByteArrayOutputStream ba = new ByteArrayOutputStream();
    	DataOutputStream da = new DataOutputStream(ba); 
    	writeObject(da,obj);
    	
    	da.close();
    	return ba.toByteArray();
    }
    
    
    
	public static void writeObject(final DataOutputStream out, final Object obj) throws IOException {
    	final int written = DEBUG?out.size():0;

    	final Class clazz = obj!=null?obj.getClass():null;
    	if(DEBUGSTORE){
    		out.writeInt(DEBUGSTORE_DUMMY_START);
    	}
    	if(obj == null){
    		out.write(NULL);    		
    	}else if (clazz ==  Boolean.class){
    		if(((Boolean)obj).booleanValue())
    			out.write(BOOLEAN_TRUE);
    		else
    			out.write(BOOLEAN_FALSE);    		
    	}else if (clazz ==  Integer.class){
    		final int val = (Integer) obj;
    		writeInteger(out, val);
		}else if (clazz ==  Double.class){
			double v = (Double) obj;
			if(v == -1d)
				out.write(DOUBLE_MINUS_1);
			else if(v == 0d)
				out.write(DOUBLE_0);
			else if(v == 1d)
				out.write(DOUBLE_1);
			else if(v >= 0&& v<=255 && (int)v == v){
				out.write(DOUBLE_255);
				out.write((int) v);						
			}else if(v >= Short.MIN_VALUE&& v<=Short.MAX_VALUE && (short)v == v){
				out.write(DOUBLE_SHORT);
				out.writeShort((int) v);			
			}else{
				out.write(DOUBLE_FULL);
				out.writeDouble(v);
			}
		}else if (clazz ==  Float.class){
			float v = (Float) obj;
			if(v == -1f)
				out.write(FLOAT_MINUS_1);
			else if(v == 0f)
				out.write(FLOAT_0);
			else if(v == 1f)
				out.write(FLOAT_1);
			else if(v >= 0&& v<=255 && (int)v == v){
				out.write(FLOAT_255);
				out.write((int) v);						
			}else if(v >= Short.MIN_VALUE&& v<=Short.MAX_VALUE && (short)v == v){
				out.write(FLOAT_SHORT);
				out.writeShort((int) v);			
			
			}else{
				out.write(FLOAT_FULL);
				out.writeFloat(v);
			}		
		}else if (clazz ==  Long.class){
			final long val = (Long) obj;
    		writeLong(out, val);
		}else if (clazz ==  Short.class){
			short val = (Short)obj;
			if(val == -1)
				out.write(SHORT_MINUS_1);
			else if(val == 0)
				out.write(SHORT_0);
			else if(val == 1)
				out.write(SHORT_1);
			else if(val > 0 && val<255){
				out.write(SHORT_255);
				out.write(val);
			}else{
				out.write(SHORT_FULL);
				out.writeShort(val);
			}					
		}else if (clazz ==  Byte.class){
			byte val = (Byte)obj;
			if(val == -1)
				out.write(BYTE_MINUS_1);
			else if(val == 0)
				out.write(BYTE_0);
			else if(val == 1)
				out.write(BYTE_1);
			else{
				out.write(SHORT_FULL);
				out.writeByte(val);
			}
    	}else if (clazz ==  Character.class){
    		out.write(CHAR);
    		out.writeChar((Character)obj);
		}else if (clazz ==  BlockIo.class){
			out.write(BLOCKIO);
			((BlockIo)obj).writeExternal(out);
		}else if (clazz ==  StoreReference.class){
			out.write(STOREREFERENCE);
			((StoreReference)obj).writeExternal(out);			
		}else if(clazz == String.class){
			byte[] s = ((String)obj).getBytes();
			if(s.length==0){
				out.write(STRING_EMPTY);
			}else if(s.length<255){
				out.write(STRING_255);
				out.write(s.length);
			}else{
				out.write(STRING);
				LongPacker.packInt(out, s.length);
			}
			out.write(s);
		}else if(obj instanceof Class){
			out.write(CLASS);
			writeObject(out, ((Class)obj).getName());
		}else if(obj instanceof int[]){
			writeIntArray(out,(int[])obj);
		}else if(obj instanceof long[]){
			writeLongArray(out,(long[])obj);		
		}else if(obj instanceof byte[]){
			byte[] b = (byte[]) obj;
			if(b.length<=255){
				out.write(ARRAY_BYTE_255);
				out.write(b.length);
			}else{
				out.write(ARRAY_BYTE_INT);
				LongPacker.packInt(out,b.length);
			}
			out.write(b);

		}else if(obj instanceof Object[]){
			Object[] b = (Object[]) obj;
			if(b.length<=255){
				//check if it contains packable longs
				boolean packableLongs = true;
				for(Object o:b){
					if(o!=null && (o.getClass() != Long.class || (((Long)o).longValue()<0 && ((Long)o).longValue()!=Long.MAX_VALUE))){
						packableLongs = false;
						break;
					}					
				}
				if(packableLongs){
					out.write(ARRAY_OBJECT_PACKED_LONG);
					out.write(b.length);
					for(Object o : b){
						if(o == null)
							LongPacker.packLong(out,0);
						else
							LongPacker.packLong(out,((Long)o).longValue()+1);
					}

				}else{				
					out.write(ARRAY_OBJECT_255);
					out.write(b.length);
					for(Object o : b)
						writeObject(out,o);
				}

			}else{
				out.write(ARRAY_OBJECT);
				LongPacker.packInt(out,b.length);
				for(Object o : b)
					writeObject(out,o);

			}
			
		}else if(clazz ==  ArrayList.class){
			ArrayList l = (ArrayList) obj;
			if(l.size()<255){
				//check if it contains packable longs
				boolean packableLongs = true;
				for(Object o:l){
					if(o!=null && (o.getClass() != Long.class || (((Long)o).longValue()<0 && ((Long)o).longValue()!=Long.MAX_VALUE))){
						packableLongs = false;
						break;
					}					
				}
				if(packableLongs){
					out.write(ARRAYLIST_PACKED_LONG);
					out.write(l.size());
					for(Object o : l){
						if(o == null)
							LongPacker.packLong(out,0);
						else
							LongPacker.packLong(out,((Long)o).longValue()+1);
					}
				}else{
					out.write(ARRAYLIST_255);
					out.write(l.size());
					for(Object o:l)
						writeObject(out, o);					
				}

			}else{
				out.write(ARRAYLIST);
				LongPacker.packInt(out,l.size());
				for(Object o:l)
					writeObject(out, o);
			}

		}else if(clazz ==  LinkedList.class){
			LinkedList l = (LinkedList) obj;
			if(l.size()<255){
				out.write(LINKEDLIST_255);
				out.write(l.size());
			}else{
				out.write(LINKEDLIST);
				LongPacker.packInt(out,l.size());
			}

			for(Object o:l)
				writeObject(out, o);
		}else if(clazz ==  Vector.class){
			Vector l = (Vector) obj;
			if(l.size()<255){
				out.write(VECTOR_255);
				out.write(l.size());
			}else{
				out.write(VECTOR);
				LongPacker.packInt(out,l.size());
			}

			for(Object o:l)
				writeObject(out, o);
		}else if(clazz ==  TreeSet.class){
			TreeSet l = (TreeSet) obj;
			if(l.size()<255){
				out.write(TREESET_255);
				out.write(l.size());
			}else{
				out.write(TREESET);
				LongPacker.packInt(out,l.size());
			}
			writeObject(out,l.comparator());

			for(Object o:l)
				writeObject(out, o);
		}else if(clazz ==  HashSet.class){
			HashSet l = (HashSet) obj;
			if(l.size()<255){
				out.write(HASHSET_255);
				out.write(l.size());
			}else{
				out.write(HASHSET);
				LongPacker.packInt(out,l.size());
			}

			for(Object o:l)
				writeObject(out, o);
		}else if(clazz ==  LinkedHashSet.class){
			LinkedHashSet l = (LinkedHashSet) obj;
			if(l.size()<255){
				out.write(LINKEDHASHSET_255);
				out.write(l.size());
			}else{
				out.write(LINKEDHASHSET);
				LongPacker.packInt(out,l.size());
			}

			for(Object o:l)
				writeObject(out, o);
		}else if(clazz ==  TreeMap.class){
			TreeMap l = (TreeMap) obj;
			if(l.size()<255){
				out.write(TREEMAP_255);
				out.write(l.size());
			}else{
				out.write(TREEMAP);
				LongPacker.packInt(out,l.size());
			}

			writeObject(out, l.comparator());
			for(Object o:l.keySet()){
				writeObject(out, o);
				writeObject(out, l.get(o));
			}
		}else if(clazz ==  HashMap.class){
			HashMap l = (HashMap) obj;
			if(l.size()<255){
				out.write(HASHMAP_255);
				out.write(l.size());
			}else{
				out.write(HASHMAP);
				LongPacker.packInt(out,l.size());
			}

			for(Object o:l.keySet()){
				writeObject(out, o);
				writeObject(out, l.get(o));
			}			
		}else if(clazz ==  LinkedHashMap.class){
			LinkedHashMap l = (LinkedHashMap) obj;
			if(l.size()<255){
				out.write(LINKEDHASHMAP_255);
				out.write(l.size());
			}else{
				out.write(LINKEDHASHMAP);
				LongPacker.packInt(out,l.size());
			}

			for(Object o:l.keySet()){
				writeObject(out, o);
				writeObject(out, l.get(o));
			}					
		}else if(clazz ==  Hashtable.class){
			Hashtable l = (Hashtable) obj;
			if(l.size()<255){
				out.write(HASHTABLE_255);
				out.write(l.size());
			}else{
				out.write(HASHTABLE);
				LongPacker.packInt(out,l.size());
			}

			for(Object o:l.keySet()){
				writeObject(out, o);
				writeObject(out, l.get(o));
			}					
			
		}else if(clazz ==  Properties.class){
			Properties l = (Properties) obj;
			if(l.size()<255){
				out.write(PROPERTIES_255);
				out.write(l.size());
			}else{
				out.write(PROPERTIES);
				LongPacker.packInt(out,l.size());
			}

			for(Object o:l.keySet()){
				writeObject(out, o);
				writeObject(out, l.get(o));
			}					
			
		}else{
			out.write(serializeNormal(obj));
			out.writeByte(END_OF_NORMAL_SERIALIZATION);
		}
    	
    	if(DEBUGSTORE){
    		out.writeInt(DEBUGSTORE_DUMMY_END);
    	}

    	if(DEBUG){
    		System.out.println("SERIAL write object: "+(clazz!=null?clazz.getSimpleName():"null")+ " - " +(out.size() - written)+"B - "+obj);
    	}
	}


	private static void writeLongArray(DataOutputStream da, long[] obj) throws IOException {
		long max = Long.MIN_VALUE;
		long min = Long.MAX_VALUE;
		for(long i:obj){
			max = Math.max(max, i);
			min = Math.min(min, i);
		}

		if(0>=min && max<=255){
			da.write(ARRAY_LONG_B);
			LongPacker.packInt(da,obj.length);
			for(long l : obj)
				da.write((int) l);
		}else if(0>=min && max<=Long.MAX_VALUE){
			da.write(ARRAY_LONG_PACKED);
			LongPacker.packInt(da,obj.length);
			for(long l : obj)
				LongPacker.packLong(da, l);			
		}else if(Short.MIN_VALUE>=min && max<=Short.MAX_VALUE){
			da.write(ARRAY_LONG_S);
			LongPacker.packInt(da,obj.length);
			for(long l : obj)
				da.writeShort((short) l);			
		}else if(Integer.MIN_VALUE>=min && max<=Integer.MAX_VALUE){
			da.write(ARRAY_LONG_I);
			LongPacker.packInt(da,obj.length);
			for(long l : obj)
				da.writeInt((int) l);			
		}else{
			da.write(ARRAY_LONG_L);
			LongPacker.packInt(da,obj.length);
			for(long l : obj)
				da.writeLong(l);
		}
		
	}


	private static void writeIntArray(DataOutputStream da, int[] obj) throws IOException {
		int max = Integer.MIN_VALUE;
		int min = Integer.MAX_VALUE;
		for(int i:obj){
			max = Math.max(max, i);
			min = Math.min(min, i);
		}
		
		boolean fitsInByte = 0>=min && max<=255;
		boolean fitsInShort = Short.MIN_VALUE>=min && max<=Short.MAX_VALUE;


		if(obj.length<=255 && fitsInByte){
			da.write(ARRAY_INT_B_255);
			da.write(obj.length);
			for(int i:obj)				
				da.write(i);
		}else if(fitsInByte){
			da.write(ARRAY_INT_B_INT);
			LongPacker.packInt(da,obj.length);
			for(int i:obj)
				da.write(i);
		}else if(0>=min && max<=Integer.MAX_VALUE){
			da.write(ARRAY_INT_PACKED);
			LongPacker.packInt(da,obj.length);
			for(int l : obj)
				LongPacker.packInt(da, l);			
		} else if(fitsInShort){
			da.write(ARRAY_INT_S);
			LongPacker.packInt(da,obj.length);
			for(int i:obj)
				da.writeShort(i);
		}else{
			da.write(ARRAY_INT_S);
			LongPacker.packInt(da,obj.length);
			for(int i:obj)
				da.writeInt(i);									
		}
				
	}


	private static void writeInteger(DataOutputStream da, final int val) throws IOException {
		if(val == -1)
			da.write(INTEGER_MINUS_1);
		else if (val == 0)
			da.write(INTEGER_0);
		else if (val == 1)
			da.write(INTEGER_1);
		else if (val == 2)
			da.write(INTEGER_2);
		else if (val == 3)
			da.write(INTEGER_3);
		else if (val == 4)
			da.write(INTEGER_4);
		else if (val == 5)
			da.write(INTEGER_5);
		else if (val == 6)
			da.write(INTEGER_6);
		else if (val == 7)
			da.write(INTEGER_7);
		else if (val == 8)
			da.write(INTEGER_8);
		else if (val == Integer.MIN_VALUE)
			da.write(INTEGER_MINUS_MAX);		
		else if(val >0 && val<255){
			da.write(INTEGER_255);
			da.write(val);
		}else if(val <0){
			da.write(INTEGER_PACK_NEG);
			LongPacker.packInt(da, -val);
		}else{
			da.write(INTEGER_PACK);
			LongPacker.packInt(da, val);
		}
	}

	private static void writeLong(DataOutputStream da, final long val) throws IOException {
		if(val == -1)
			da.write(LONG_MINUS_1);
		else if (val == 0)
			da.write(LONG_0);
		else if (val == 1)
			da.write(LONG_1);
		else if (val == 2)
			da.write(LONG_2);
		else if (val == 3)
			da.write(LONG_3);
		else if (val == 4)
			da.write(LONG_4);
		else if (val == 5)
			da.write(LONG_5);
		else if (val == 6)
			da.write(LONG_6);
		else if (val == 7)
			da.write(LONG_7);
		else if (val == 8)
			da.write(LONG_8);
		else if (val == Long.MIN_VALUE)
			da.write(LONG_MINUS_MAX);		
		else if(val >0 && val<255){
			da.write(LONG_255);
			da.write((int) val);
		}else if(val <0){
			da.write(LONG_PACK_NEG);
			LongPacker.packLong(da, -val);
		}else{
			da.write(LONG_PACK);
			LongPacker.packLong(da, val);
		}
	}


	/**
     * Deserialize an object from a byte array
     * @throws IOException 
     * @throws ClassNotFoundException 
     */
    public static Object deserialize( byte[] buf ) throws ClassNotFoundException, IOException{
    	ByteArrayInputStream bs =  new ByteArrayInputStream(buf);
    	DataInputStream das = new DataInputStream(bs);
    	Object ret = readObject(das);
		if(bs.available()!=0)
			throw new InternalError("bytes left: "+bs.available());

    	return ret;
    }

    
    private static String deserializeString(DataInputStream buf) throws IOException {
    	int len  = LongPacker.unpackInt(buf);
    	byte[] b=  new byte[len];
    	buf.readFully(b);
    	return new String(b);
	}

	private static String deserializeString256Smaller(DataInputStream buf) throws IOException {
    	int len  = buf.read();
    	if (len < 0)
    	    throw new EOFException();

    	byte[] b=  new byte[len];
    	buf.readFully(b);
    	return new String(b);    	
}
	/**
     * Serialize the object into a byte array.
     */
    protected static byte[] serializeNormal( Object obj )
        throws IOException
    {
    	
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(NORMAL);
        ObjectOutputStream oos = new ObjectOutputStream( baos );
        oos.writeObject( obj );
        oos.close();
        
        
        return baos.toByteArray();
    }


    /**
     * Deserialize an object from a byte array
     */
    protected static Object deserializeNormal(DataInputStream buf )
        throws ClassNotFoundException, IOException
    {

        ObjectInputStream ois = new ObjectInputStream( buf );
        Object ret =  ois.readObject();
        if(buf.readByte()!=END_OF_NORMAL_SERIALIZATION)
        	throw new IOException("Wrong magic after serialization, maybe is Externalizable and wrong amount of bytes was read?");
        return ret;
    }

    public static Object readObject(DataInputStream is) throws IOException, ClassNotFoundException{
    	final int available = DEBUG?is.available():0;

    	Object ret = null;
    	
    	if(DEBUGSTORE && is.readInt()!=DEBUGSTORE_DUMMY_START){    		
    		throw new InternalError("Wrong offset");
    	}    	
    	
    	int head = is.read();

    	switch(head){
    		case NULL:ret=  null;break;
			case NORMAL:ret= deserializeNormal(is);break;
			case BOOLEAN_TRUE:ret= true;break;
			case BOOLEAN_FALSE:ret= false;break;
			case INTEGER_MINUS_1:ret= Integer.valueOf(-1);break;
			case INTEGER_0:ret= Integer.valueOf(0);break;
			case INTEGER_1:ret= Integer.valueOf(1);break;
			case INTEGER_2:ret= Integer.valueOf(2);break;
			case INTEGER_3:ret= Integer.valueOf(3);break;
			case INTEGER_4:ret= Integer.valueOf(4);break;
			case INTEGER_5:ret= Integer.valueOf(5);break;
			case INTEGER_6:ret= Integer.valueOf(6);break;
			case INTEGER_7:ret= Integer.valueOf(7);break;
			case INTEGER_8:ret= Integer.valueOf(8);break;
			case INTEGER_MINUS_MAX:ret=  Integer.valueOf(Integer.MIN_VALUE);break;
			case INTEGER_255:ret= Integer.valueOf(is.read());break;
			case INTEGER_PACK_NEG:ret=  Integer.valueOf(-LongPacker.unpackInt(is));break;
			case INTEGER_PACK:ret=  Integer.valueOf(LongPacker.unpackInt(is));break;
			case LONG_MINUS_1:ret= Long.valueOf(-1);break;
			case LONG_0:ret= Long.valueOf(0);break;
			case LONG_1:ret= Long.valueOf(1);break;
			case LONG_2:ret= Long.valueOf(2);break;
			case LONG_3:ret= Long.valueOf(3);break;
			case LONG_4:ret= Long.valueOf(4);break;
			case LONG_5:ret= Long.valueOf(5);break;
			case LONG_6:ret= Long.valueOf(6);break;
			case LONG_7:ret= Long.valueOf(7);break;
			case LONG_8:ret= Long.valueOf(8);break;
			case LONG_255:ret= Long.valueOf(is.read());break;
			case LONG_PACK_NEG:ret=  Long.valueOf(-LongPacker.unpackLong(is));break;
			case LONG_PACK:ret=  Long.valueOf(LongPacker.unpackLong(is));break;
			case LONG_MINUS_MAX:ret=  Long.valueOf(Long.MIN_VALUE);break;
			case SHORT_MINUS_1:ret= Short.valueOf((short)-1);break;
			case SHORT_0:ret= Short.valueOf((short)0);break;
			case SHORT_1:ret= Short.valueOf((short)1);break;
			case SHORT_255:ret= Short.valueOf((short)is.read());break;
			case SHORT_FULL:ret= Short.valueOf(is.readShort());break;
			case BYTE_MINUS_1:ret= Byte.valueOf((byte)-1);break;
			case BYTE_0:ret= Byte.valueOf((byte)0);break;
			case BYTE_1:ret= Byte.valueOf((byte)1);break;
			case BYTE_FULL:ret= Byte.valueOf(is.readByte());break;
			case CHAR:ret= Character.valueOf(is.readChar());break;
			case FLOAT_MINUS_1:ret= Float.valueOf(-1);break;
			case FLOAT_0:ret= Float.valueOf(0);break;
			case FLOAT_1:ret= Float.valueOf(1);break;
			case FLOAT_255:ret= Float.valueOf(is.read());break;
			case FLOAT_SHORT:ret=  Float.valueOf(is.readShort());break;
			case FLOAT_FULL:ret= Float.valueOf(is.readFloat());break;
			case DOUBLE_MINUS_1:ret= Double.valueOf(-1);break;
			case DOUBLE_0:ret= Double.valueOf(0);break;
			case DOUBLE_1:ret= Double.valueOf(1);break;
			case DOUBLE_255:ret= Double.valueOf(is.read());break;
			case DOUBLE_SHORT:ret= Double.valueOf(is.readShort());break;
			case DOUBLE_FULL:ret= Double.valueOf(is.readDouble());break;			
			case BLOCKIO:ret= deserializeBlockIo(is);break;
			case STOREREFERENCE:ret= deserializeStoreReference(is);break;
			case STRING_255:ret= deserializeString256Smaller(is);break;
			case STRING:ret= deserializeString(is);break;
			case STRING_EMPTY:ret= "";break;
			case ARRAYLIST_255:ret= deserializeArrayList256Smaller(is);break;
			case ARRAYLIST:ret= deserializeArrayList(is);break;
			case ARRAYLIST_PACKED_LONG:ret= deserializeArrayListPackedLong(is);break;
			case ARRAY_OBJECT_255:ret= deserializeArrayObject256Smaller(is);break;
			case ARRAY_OBJECT:ret= deserializeArrayObject(is);break;
			case ARRAY_OBJECT_PACKED_LONG:ret= deserializeArrayObjectPackedLong(is);break;
			case LINKEDLIST_255:ret= deserializeLinkedList256Smaller(is);break;
			case LINKEDLIST:ret= deserializeLinkedList(is);break;
			case TREESET_255:ret= deserializeTreeSet256Smaller(is);break;
			case TREESET:ret= deserializeTreeSet(is);break;
			case HASHSET_255:ret= deserializeHashSet256Smaller(is);break;
			case HASHSET:ret= deserializeHashSet(is);break;
			case LINKEDHASHSET_255:ret= deserializeLinkedHashSet256Smaller(is);break;
			case LINKEDHASHSET:ret= deserializeLinkedHashSet(is);break;
			case VECTOR_255:ret= deserializeVector256Smaller(is);break;
			case VECTOR:ret= deserializeVector(is);break;
			case TREEMAP_255:ret= deserializeTreeMap256Smaller(is);break;
			case TREEMAP:ret= deserializeTreeMap(is);break;
			case HASHMAP_255:ret= deserializeHashMap256Smaller(is);break;
			case HASHMAP:ret= deserializeHashMap(is);break;
			case LINKEDHASHMAP_255:ret= deserializeLinkedHashMap256Smaller(is);break;
			case LINKEDHASHMAP:ret= deserializeLinkedHashMap(is);break;
			case HASHTABLE_255:ret= deserializeHashtable256Smaller(is);break;
			case HASHTABLE:ret= deserializeHashtable(is);break;
			case PROPERTIES_255:ret= deserializeProperties256Smaller(is);break;
			case PROPERTIES:ret= deserializeProperties(is);break;
			case CLASS:ret= deserializeClass(is);break;
			
			
			case ARRAY_INT_B_255: ret= deserializeArrayIntB255(is);break;
			case ARRAY_INT_B_INT: ret= deserializeArrayIntBInt(is);break;
			case ARRAY_INT_S: ret= deserializeArrayIntSInt(is);break;
			case ARRAY_INT_I: ret= deserializeArrayIntIInt(is);break;
			case ARRAY_INT_PACKED: ret= deserializeArrayIntPack(is);break;
			case ARRAY_LONG_B: ret= deserializeArrayLongB(is);break;
			case ARRAY_LONG_S: ret= deserializeArrayLongS(is);break;
			case ARRAY_LONG_I: ret= deserializeArrayLongI(is);break;
			case ARRAY_LONG_L: ret= deserializeArrayLongL(is);break;
			case ARRAY_LONG_PACKED: ret= deserializeArrayLongPack(is);break;
			case ARRAY_BYTE_255: ret= deserializeArrayByte255(is);break;
			case ARRAY_BYTE_INT: ret= deserializeArrayByteInt(is);break;
			case BPAGE_LEAF: throw new InternalError("BPage header, wrong serializer used");
			case BPAGE_NONLEAF: throw new InternalError("BPage header, wrong serializer used");
			case JAVA_SERIALIZATION: throw new InternalError("Wrong header, data were propably serialized with OutputStream, not with JDBM serialization");
			
			case -1: throw new EOFException();
			
			default: throw new InternalError("Unknown serialization header: "+head);
    	}
        	    	
    	if(DEBUG){
    		System.out.println("SERIAL read object: "+ret.getClass().getSimpleName()+" - "+(available-is.available())+"B - "+ ret);
    	}

    	if(DEBUGSTORE && is.readInt()!=DEBUGSTORE_DUMMY_END){
    		throw new InternalError("Wrong offset '"+ret+ "' - "+ret.getClass());
    	}

    
    	return ret;
	}


	private static Class deserializeClass(DataInputStream is) throws IOException, ClassNotFoundException {
		String className = (String) readObject(is);
		Class cls = Class.forName(className);
		return cls;
	}



	private static StoreReference deserializeStoreReference(DataInputStream is) throws IOException {
		StoreReference r = new StoreReference();
		r.readExternal(is);
		return r;
	}



	private static byte[] deserializeArrayByteInt(DataInputStream is) throws IOException {
		int size = LongPacker.unpackInt(is);
		byte[] b = new byte[size];
		is.readFully(b);
		return b;
	}


	private static byte[] deserializeArrayByte255(DataInputStream is) throws IOException {
		int size = is.read();
    	if (size < 0)
    	    throw new EOFException();

		byte[] b = new byte[size];
		is.readFully(b);
		return b;

	}


	private static long[] deserializeArrayLongL(DataInputStream is) throws IOException {
		int size = LongPacker.unpackInt(is);
		long[] ret = new long[size];
		for(int i=0;i<size;i++)
			ret[i] = is.readLong();
		return ret;	
	}


	private static long[] deserializeArrayLongI(DataInputStream is) throws IOException {
		int size = LongPacker.unpackInt(is);
		long[] ret = new long[size];
		for(int i=0;i<size;i++)
			ret[i] = is.readInt();
		return ret;	
	}


	private static long[] deserializeArrayLongS(DataInputStream is) throws IOException {
		int size = LongPacker.unpackInt(is);
		long[] ret = new long[size];
		for(int i=0;i<size;i++)
			ret[i] = is.readShort();
		return ret;	
	}


	private static long[] deserializeArrayLongB(DataInputStream is) throws IOException {
		int size = LongPacker.unpackInt(is);
		long[] ret = new long[size];
		for(int i=0;i<size;i++){
			ret[i] = is.read();
			if(ret[i] <0)
	    	    throw new EOFException();
		}
		return ret;
	}


	private static int[] deserializeArrayIntIInt(DataInputStream is) throws IOException {
		int size = LongPacker.unpackInt(is);
		int[] ret = new int[size];
		for(int i=0;i<size;i++)
			ret[i] = is.readInt();
		return ret;
	}


	private static int[] deserializeArrayIntSInt(DataInputStream is) throws IOException {
		int size = LongPacker.unpackInt(is);
		int[] ret = new int[size];
		for(int i=0;i<size;i++)
			ret[i] = is.readShort();
		return ret;
	}



	private static int[] deserializeArrayIntBInt(DataInputStream is) throws IOException {
		int size = LongPacker.unpackInt(is);
		int[] ret = new int[size];
		for(int i=0;i<size;i++){
			ret[i] = is.read();
			if(ret[i] <0)
	    	    throw new EOFException();
		}
		return ret;	}


	private static int[] deserializeArrayIntPack(DataInputStream is) throws IOException {
		int size = LongPacker.unpackInt(is);
    	if (size < 0)
    	    throw new EOFException();

		int[] ret = new int[size];
		for(int i=0;i<size;i++){
			ret[i] = LongPacker.unpackInt(is);
		}
		return ret;	
	}
	
	private static long[] deserializeArrayLongPack(DataInputStream is) throws IOException {
		int size = LongPacker.unpackInt(is);
    	if (size < 0)
    	    throw new EOFException();

		long[] ret = new long[size];
		for(int i=0;i<size;i++){
			ret[i] = LongPacker.unpackLong(is);
		}
		return ret;	
	}

	private static int[] deserializeArrayIntB255(DataInputStream is) throws IOException {
		int size = is.read();
    	if (size < 0)
    	    throw new EOFException();

		int[] ret = new int[size];
		for(int i=0;i<size;i++){
			ret[i] = is.read();
			if(ret[i] <0)
	    	    throw new EOFException();
		}
		return ret;	}


	private static BlockIo deserializeBlockIo(DataInputStream is) throws IOException, ClassNotFoundException {
		BlockIo b = new BlockIo();
		b.readExternal(is);
		return b;
	}
	
	private static Object[] deserializeArrayObject(DataInputStream is) throws IOException, ClassNotFoundException {
		int size =LongPacker.unpackInt(is);
		Object[] s = new Object[size];
		for(int i = 0; i<size;i++)
			s[i] = readObject(is);
		return s;
	}
	
	private static Object[] deserializeArrayObjectPackedLong(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = is.read();
		Object[] s = new Object[size];
		for(int i = 0; i<size;i++){
			long l = LongPacker.unpackLong(is);
			if(l == 0)
				s[i] = null;
			else
				s[i] = Long.valueOf(l-1);			
		}
		return s;
	}


	private static Object[] deserializeArrayObject256Smaller(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = is.read();
		if(size <0)
    	    throw new EOFException();

		Object[] s = new Object[size];
		for(int i = 0; i<size;i++)
			s[i] = readObject(is);
		return s;
	}

	private static ArrayList<Object> deserializeArrayList(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);
		ArrayList<Object> s = new ArrayList<Object>(size);
		for(int i = 0; i<size;i++)
			s.add(readObject(is));
		return s;
	}
	
	private static ArrayList<Object> deserializeArrayListPackedLong(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = is.read();
		if(size <0)
    	    throw new EOFException();

		ArrayList<Object> s = new ArrayList<Object>(size);
		for(int i = 0; i<size;i++){
			long l = LongPacker.unpackLong(is);
			if(l == 0)
				s.add(null);
			else
				s.add( Long.valueOf(l-1));			
		}
		return s;
	}

	private static ArrayList<Object> deserializeArrayList256Smaller(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = is.read();
		if(size <0)
    	    throw new EOFException();

		ArrayList<Object> s = new ArrayList<Object>(size);
		for(int i = 0; i<size;i++)
			s.add(readObject(is));
		return s;
	}

	private static LinkedList<Object> deserializeLinkedList(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);
		LinkedList<Object> s = new LinkedList<Object>();
		for(int i = 0; i<size;i++)
			s.add(readObject(is));
		return s;
	}

	private static LinkedList<Object> deserializeLinkedList256Smaller(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = is.read();
		if(size <0)
    	    throw new EOFException();

		LinkedList<Object> s = new LinkedList<Object>();
		for(int i = 0; i<size;i++)
			s.add(readObject(is));
		return s;
	}
	
	private static Vector<Object> deserializeVector(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);
		Vector<Object> s = new Vector<Object>(size);
		for(int i = 0; i<size;i++)
			s.add(readObject(is));
		return s;
	}

	private static Vector<Object> deserializeVector256Smaller(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = is.read();
		if(size <0)
    	    throw new EOFException();

		Vector<Object> s = new Vector<Object>(size);
		for(int i = 0; i<size;i++)
			s.add(readObject(is));
		return s;
	}
	
	private static HashSet<Object> deserializeHashSet(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);
		HashSet<Object> s = new HashSet<Object>(size);
		for(int i = 0; i<size;i++)
			s.add(readObject(is));
		return s;
	}

	private static HashSet<Object> deserializeHashSet256Smaller(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = is.read();
		if(size <0)
    	    throw new EOFException();

		HashSet<Object> s = new HashSet<Object>(size);
		for(int i = 0; i<size;i++)
			s.add(readObject(is));
		return s;
	}

	private static LinkedHashSet<Object> deserializeLinkedHashSet(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);
		LinkedHashSet<Object> s = new LinkedHashSet<Object>(size);
		for(int i = 0; i<size;i++)
			s.add(readObject(is));
		return s;
	}

	private static LinkedHashSet<Object> deserializeLinkedHashSet256Smaller(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = is.read();
		if(size <0)
    	    throw new EOFException();

		LinkedHashSet<Object> s = new LinkedHashSet<Object>(size);
		for(int i = 0; i<size;i++)
			s.add(readObject(is));
		return s;
	}
	
	
	private static TreeSet<Object> deserializeTreeSet(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);		
		TreeSet<Object> s = new TreeSet<Object>();
		Comparator comparator = (Comparator) readObject(is);
		if(comparator!=null)
			s = new TreeSet<Object>(comparator);
		
		for(int i = 0; i<size;i++)
			s.add(readObject(is));
		return s;
	}

	private static TreeSet<Object> deserializeTreeSet256Smaller(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = is.read();
		if(size <0)
    	    throw new EOFException();

		TreeSet<Object> s = new TreeSet<Object>();
		Object obj = readObject(is);
		Comparator comparator = (Comparator) obj;
		if(comparator!=null)
			s = new TreeSet<Object>(comparator);
		for(int i = 0; i<size;i++)
			s.add(readObject(is));
		return s;
	}

	
	private static TreeMap<Object,Object> deserializeTreeMap(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);		

		TreeMap<Object,Object> s = new TreeMap<Object,Object>();
		Comparator comparator = (Comparator) readObject(is);
		if(comparator!=null)
			s = new TreeMap<Object,Object>(comparator);
		for(int i = 0; i<size;i++)
			s.put(readObject(is),readObject(is));
		return s;
	}

	private static TreeMap<Object,Object> deserializeTreeMap256Smaller(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = is.read();
		if(size <0)
    	    throw new EOFException();

		TreeMap<Object,Object> s = new TreeMap<Object,Object>();
		Comparator comparator = (Comparator) readObject(is);
		if(comparator!=null)
			s = new TreeMap<Object,Object>(comparator);
		for(int i = 0; i<size;i++)
			s.put(readObject(is),readObject(is));
		return s;
	}

	
	private static HashMap<Object,Object> deserializeHashMap(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);		

		HashMap<Object,Object> s = new HashMap<Object,Object>(size);
		for(int i = 0; i<size;i++)
			s.put(readObject(is),readObject(is));
		return s;
	}

	private static HashMap<Object,Object> deserializeHashMap256Smaller(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = is.read();
		if(size <0)
    	    throw new EOFException();

		HashMap<Object,Object> s = new HashMap<Object,Object>(size);
		for(int i = 0; i<size;i++)
			s.put(readObject(is),readObject(is));
		return s;
	}
	
	
	private static LinkedHashMap<Object,Object> deserializeLinkedHashMap(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);		

		LinkedHashMap<Object,Object> s = new LinkedHashMap<Object,Object>(size);
		for(int i = 0; i<size;i++)
			s.put(readObject(is),readObject(is));
		return s;
	}

	private static LinkedHashMap<Object,Object> deserializeLinkedHashMap256Smaller(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = is.read();
		if(size <0)
    	    throw new EOFException();

		LinkedHashMap<Object,Object> s = new LinkedHashMap<Object,Object>(size);
		for(int i = 0; i<size;i++)
			s.put(readObject(is),readObject(is));
		return s;
	}

	
	private static Hashtable<Object,Object> deserializeHashtable(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);		

		Hashtable<Object,Object> s = new Hashtable<Object,Object>(size);
		for(int i = 0; i<size;i++)
			s.put(readObject(is),readObject(is));
		return s;
	}

	private static Hashtable<Object,Object> deserializeHashtable256Smaller(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = is.read();
		if(size <0)
    	    throw new EOFException();

		Hashtable<Object,Object> s = new Hashtable<Object,Object>(size);
		for(int i = 0; i<size;i++)
			s.put(readObject(is),readObject(is));
		return s;
	}
	
	
	private static Properties deserializeProperties(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);		

		Properties s = new Properties();
		for(int i = 0; i<size;i++)
			s.put(readObject(is),readObject(is));
		return s;
	}

	private static Properties deserializeProperties256Smaller(DataInputStream is) throws IOException, ClassNotFoundException {
		int size = is.read();
		if(size <0)
    	    throw new EOFException();

		Properties s = new Properties();
		for(int i = 0; i<size;i++)
			s.put(readObject(is),readObject(is));
		return s;
	}	
}
