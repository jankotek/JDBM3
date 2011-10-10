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
package jdbm;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

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
class Serialization extends SerialClassInfo implements Serializer
{


	static final byte END_OF_NORMAL_SERIALIZATION = 111;

	/** print statistics to STDOUT */
	static final boolean DEBUG = false;

	/** if set to true, debug informations will be saved to store to make it more robust */
	static final boolean DEBUGSTORE = false;
	
	private static final int DEBUGSTORE_DUMMY_START = 456456567;
	private static final int DEBUGSTORE_DUMMY_END = 1234456;
	
	final static int NULL 			=   0;
	final static int NORMAL 			=   1;
	final static int BOOLEAN_TRUE 	=   2;
	final static int BOOLEAN_FALSE 	=   3;
	final static int INTEGER_MINUS_1 =   4;
	final static int INTEGER_0 		=   5;
	final static int INTEGER_1 		=   6;
	final static int INTEGER_2 		=   7;
	final static int INTEGER_3 		=   8;
	final static int INTEGER_4 		=   9;
	final static int INTEGER_5 		=  10;
	final static int INTEGER_6 		=  11;
	final static int INTEGER_7 		=  12;
	final static int INTEGER_8 		=  13;
	final static int INTEGER_255		=  14;	
	final static int INTEGER_PACK_NEG=  15;
	final static int INTEGER_PACK 	=  16;
	final static int LONG_MINUS_1 	=  17;
	final static int LONG_0 			=  18;
	final static int LONG_1 			=  19;
	final static int LONG_2 			=  20;
	final static int LONG_3 			=  21;
	final static int LONG_4 			=  22;
	final static int LONG_5 			=  23;
	final static int LONG_6 			=  24;
	final static int LONG_7 			=  25;
	final static int LONG_8 			=  26;
	final static int LONG_PACK_NEG	=  27;
	final static int LONG_PACK 		=  28;
	final static int LONG_255 		=  29;
	final static int LONG_MINUS_MAX	=  30;
	final static int SHORT_MINUS_1 	=  31;
	final static int SHORT_0 		=  32;
	final static int SHORT_1 		=  33;
	final static int SHORT_255 		=  34;
	final static int SHORT_FULL		=  35;	
	final static int BYTE_MINUS_1 	=  36;
	final static int BYTE_0 			=  37;
	final static int BYTE_1 			=  38;
	final static int BYTE_FULL		=  39;	
	final static int CHAR			=  40;
	final static int FLOAT_MINUS_1 	=  41;
	final static int FLOAT_0 		=  42;
	final static int FLOAT_1 		=  43;
	final static int FLOAT_255		=  44;
	final static int FLOAT_SHORT		=  45;		
	final static int FLOAT_FULL		=  46;	
	final static int DOUBLE_MINUS_1 	=  47;
	final static int DOUBLE_0 		=  48;
	final static int DOUBLE_1 		=  49;
	final static int DOUBLE_255		=  50;
	final static int DOUBLE_SHORT	=  51;	
	final static int DOUBLE_FULL		=  52;
	final static int FREEEE222	=  53; //TODO  free slot
	final static int BIGDECIMAL		=  54;
	final static int FREEEE222222	=  55; //TODO free slot
	final static int BIGINTEGER		=  56;
	final static int INTEGER_MINUS_MAX=  57;	
 	

	
	final static int ARRAY_INT_B_255		=  60;
	final static int ARRAY_INT_B_INT		=  61;
	final static int ARRAY_INT_S			=  62;
	final static int ARRAY_INT_I			=  63;
	final static int ARRAY_INT_PACKED	=  64;
	
	final static int ARRAY_LONG_B		=  65;
	final static int ARRAY_LONG_S		=  66;
	final static int ARRAY_LONG_I		=  67;
	final static int ARRAY_LONG_L		=  68;
	final static int ARRAY_LONG_PACKED	=  69;

	final static int NOTUSED_ARRAY_BYTE_255		=  70;
	final static int ARRAY_BYTE_INT		=  71;
	
	final static int NOTUSED_ARRAY_OBJECT_255	=  72;
	final static int ARRAY_OBJECT		=  73;
	//special cases for BTree values which stores references
	final static int ARRAY_OBJECT_PACKED_LONG =  74;
	final static int ARRAYLIST_PACKED_LONG =  75;
	
	final static int STRING_EMPTY		= 101;
	final static int NOTUSED_STRING_255			= 102;
	final static int STRING				= 103;
	final static int NOTUSED_ARRAYLIST_255		= 104;
	final static int ARRAYLIST			= 105;

	final static int NOTUSED_TREEMAP_255			= 106;
	final static int TREEMAP				= 107;
	final static int NOTUSED_HASHMAP_255			= 108;
	final static int HASHMAP				= 109;
	final static int NOTUSED_LINKEDHASHMAP_255	= 110;
	final static int LINKEDHASHMAP		= 111;
	
	final static int NOTUSED_TREESET_255			= 112;
	final static int TREESET				= 113;
	final static int NOTUSED_HASHSET_255			= 114;
	final static int HASHSET				= 115;
	final static int NOTUSED_LINKEDHASHSET_255	= 116;
	final static int LINKEDHASHSET		= 117;
	final static int NOTUSED_LINKEDLIST_255		= 118;
	final static int LINKEDLIST			= 119;
	

	final static int NOTUSED_VECTOR_255			= 120;
	final static int VECTOR				= 121;
	final static int NOTUSED_HASHTABLE_255		= 122;
	final static int HASHTABLE			= 123;
	final static int NOTUSED_PROPERTIES_255		= 124;
	final static int PROPERTIES			= 125;
	
	final static int CLASS				= 126;

        final static int DATE				= 127;

	final static int BTREE				= 161;

	static final int BPAGE_LEAF 			= 162;
	static final int BPAGE_NONLEAF 		= 163;
	static final int HTREE_BUCKET 		= 164;
	static final int HTREE_DIRECTORY 	= 165;
    /** used for reference to already serialized object in object graph*/
    static final int OBJECT_STACK 	= 166;
	static final int JAVA_SERIALIZATION 	= 172;


        private static final String EMPTY_STRING = "";
        private static final String UTF8 = "UTF-8";
	
    /**
     * Serialize the object into a byte array.
     */
    byte[] serialize( Object obj )
        throws IOException
    {
    	ByteArrayOutputStream ba = new ByteArrayOutputStream();
    	DataOutputStream da = new DataOutputStream(ba);
    	serialize(da,obj);

    	da.close();
    	return ba.toByteArray();
    }

    boolean isSerializable(Object obj){
        //TODO suboptimal code
        try{
           serialize(new DataOutputStream(new ByteArrayOutputStream()),obj);
           return true;
        }catch(Exception e){
            return false;
        }
    }


    public void serialize(final DataOutput out, final Object obj) throws IOException {
        serialize(out,obj,new ArrayList());
    }


    private int identityIndexOf(Object obj, ArrayList objectStack){
        for(int i=0; i<objectStack.size();i++){
            if(obj == objectStack.get(i))
                return i;
        }
        return -1;
    }
	public void serialize(final DataOutput out, final Object obj, ArrayList objectStack) throws IOException {

        int indexInObjectStack = identityIndexOf(obj,objectStack);
        if(indexInObjectStack!=-1){
            //object was already serialized, just write reference to it and return
            out.write(OBJECT_STACK);
            LongPacker.packInt(out,indexInObjectStack);
            return;
        }
        //add this object to objectStack
        objectStack.add(obj);

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
                }else if (clazz ==  BigInteger.class){
                    out.write(BIGINTEGER);
                    byte[] buf = ((BigInteger)obj).toByteArray();
                    serializeByteArrayInt(out, buf);

                }else if (clazz ==  BigDecimal.class){
                    out.write(BIGDECIMAL);
                    BigDecimal d = (BigDecimal)obj;
                    serializeByteArrayInt(out,d.unscaledValue().toByteArray());
                    LongPacker.packInt(out,d.scale());
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
    		out.writeChar((Character) obj);
		}else if(clazz == String.class){
			byte[] s = ((String)obj).getBytes(UTF8);
			if(s.length==0){
				out.write(STRING_EMPTY);
			}else{
				out.write(STRING);
				LongPacker.packInt(out, s.length);
                out.write(s);
			}
		}else if(obj instanceof Class){
			out.write(CLASS);
			serialize(out, ((Class) obj).getName());
		}else if(obj instanceof int[]){
			writeIntArray(out, (int[]) obj);
		}else if(obj instanceof long[]){
			writeLongArray(out,(long[])obj);
		}else if(obj instanceof byte[]){
			byte[] b = (byte[]) obj;
			out.write(ARRAY_BYTE_INT);
            serializeByteArrayInt(out, b);

		}else if(obj instanceof Object[]){
			Object[] b = (Object[]) obj;
            boolean packableLongs = b.length<=255;
			if(packableLongs){
				//check if it contains packable longs
				for(Object o:b){
					if(o!=null && (o.getClass() != Long.class || (((Long)o).longValue()<0 && ((Long)o).longValue()!=Long.MAX_VALUE))){
						packableLongs = false;
						break;
					}
				}
            }

            if(packableLongs){
                    //packable Longs is special case,  it is often used in JDBM to reference fields
					out.write(ARRAY_OBJECT_PACKED_LONG);
					out.write(b.length);
					for(Object o : b){
						if(o == null)
							LongPacker.packLong(out,0);
						else
							LongPacker.packLong(out,((Long)o).longValue()+1);
					}

			}else{
				out.write(ARRAY_OBJECT);
				LongPacker.packInt(out,b.length);
				for(Object o : b)
					serialize(out,o);

			}

		}else if(clazz ==  ArrayList.class){
			ArrayList l = (ArrayList) obj;
            boolean packableLongs = l.size()<255;
			if(packableLongs){
                  //packable Longs is special case,  it is often used in JDBM to reference fields
				for(Object o:l){
					if(o!=null && (o.getClass() != Long.class || (((Long)o).longValue()<0 && ((Long)o).longValue()!=Long.MAX_VALUE))){
						packableLongs = false;
						break;
					}
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
                serializeCollection(ARRAYLIST, out, obj);
			}

		}else if(clazz ==  LinkedList.class){
            serializeCollection(LINKEDLIST,out,obj);
		}else if(clazz ==  Vector.class){
            serializeCollection(VECTOR, out, obj);
		}else if(clazz ==  TreeSet.class){
			TreeSet l = (TreeSet) obj;
			out.write(TREESET);
			LongPacker.packInt(out,l.size());
			serialize(out,l.comparator());
			for(Object o:l)
				serialize(out, o);
		}else if(clazz ==  HashSet.class){
            serializeCollection(HASHSET,out,obj);
		}else if(clazz ==  LinkedHashSet.class){
            serializeCollection(LINKEDHASHSET,out,obj);
		}else if(clazz ==  TreeMap.class){
            TreeMap l = (TreeMap) obj;
            out.write(TREEMAP);
            LongPacker.packInt(out,l.size());
            serialize(out,l.comparator());
            for(Object o:l.keySet()){
                serialize(out, o);
                serialize(out, l.get(o));
            }
		}else if(clazz ==  HashMap.class){
            serializeMap(HASHMAP,out,obj);
		}else if(clazz ==  LinkedHashMap.class){
            serializeMap(LINKEDHASHMAP,out,obj);
		}else if(clazz ==  Hashtable.class){
            serializeMap(HASHTABLE,out,obj);
		}else if(clazz ==  Properties.class){
            serializeMap(PROPERTIES,out,obj);
        }else if(clazz ==  Date.class){
		    out.write(DATE);
                    out.writeLong(((Date)obj).getTime());
        }else if (clazz == BTree.class){
            out.write(BTREE);
            ((BTree)obj).writeExternal(out);
		}else{
            out.write(NORMAL);
            writeObject(out,obj, objectStack);
		}

    	if(DEBUGSTORE){
    		out.writeInt(DEBUGSTORE_DUMMY_END);
    	}
	}

    private void serializeMap(int header, DataOutput out, Object obj) throws IOException {
        Map l = (Map) obj;
		out.write(header);
		LongPacker.packInt(out,l.size());
		for(Object o:l.keySet()){
			serialize(out, o);
			serialize(out, l.get(o));
		}
    }

    private void serializeCollection(int header, DataOutput out, Object obj) throws IOException {
        Collection l = (Collection) obj;
        out.write(header);
        LongPacker.packInt(out,l.size());

        for(Object o:l)
            serialize(out, o);

    }

    private void serializeByteArrayInt(DataOutput out, byte[] b) throws IOException {
        LongPacker.packInt(out, b.length);
        out.write(b);
    }


    private void writeLongArray(DataOutput da, long[] obj) throws IOException {
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


	private void writeIntArray(DataOutput da, int[] obj) throws IOException {
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


	private void writeInteger(DataOutput da, final int val) throws IOException {
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

	private void writeLong(DataOutput da, final long val) throws IOException {
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
    Object deserialize( byte[] buf ) throws ClassNotFoundException, IOException{
    	ByteArrayInputStream bs =  new ByteArrayInputStream(buf);
    	DataInputStream das = new DataInputStream(bs);
    	Object ret = deserialize(das);
		if(bs.available()!=0)
			throw new InternalError("bytes left: "+bs.available());

    	return ret;
    }


    private String deserializeString(DataInput buf) throws IOException {
    	int len  = LongPacker.unpackInt(buf);
    	byte[] b=  new byte[len];
    	buf.readFully(b);
    	return new String(b,UTF8);
	}


    public Object deserialize(DataInput is) throws IOException, ClassNotFoundException{
        return deserialize(is, new ArrayList());
    }
    public Object deserialize(DataInput is, ArrayList objectStack) throws IOException, ClassNotFoundException{

    	Object ret = null;

    	if(DEBUGSTORE && is.readInt()!=DEBUGSTORE_DUMMY_START){
    		throw new InternalError("Wrong offset");
    	}

    	int head = is.readUnsignedByte();

    	switch(head){
    		case NULL:ret=  null;break;
            case NORMAL: ret = readObject(is,objectStack); break;
            case OBJECT_STACK: ret = objectStack.get(LongPacker.unpackInt(is)); break;
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
			case INTEGER_255:ret= Integer.valueOf(is.readUnsignedByte());break;
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
			case LONG_255:ret= Long.valueOf(is.readUnsignedByte());break;
			case LONG_PACK_NEG:ret=  Long.valueOf(-LongPacker.unpackLong(is));break;
			case LONG_PACK:ret=  Long.valueOf(LongPacker.unpackLong(is));break;
			case LONG_MINUS_MAX:ret=  Long.valueOf(Long.MIN_VALUE);break;
			case SHORT_MINUS_1:ret= Short.valueOf((short)-1);break;
			case SHORT_0:ret= Short.valueOf((short)0);break;
			case SHORT_1:ret= Short.valueOf((short)1);break;
			case SHORT_255:ret= Short.valueOf((short)is.readUnsignedByte());break;
			case SHORT_FULL:ret= Short.valueOf(is.readShort());break;
			case BYTE_MINUS_1:ret= Byte.valueOf((byte)-1);break;
			case BYTE_0:ret= Byte.valueOf((byte)0);break;
			case BYTE_1:ret= Byte.valueOf((byte)1);break;
			case BYTE_FULL:ret= Byte.valueOf(is.readByte());break;
			case CHAR:ret= Character.valueOf(is.readChar());break;
			case FLOAT_MINUS_1:ret= Float.valueOf(-1);break;
			case FLOAT_0:ret= Float.valueOf(0);break;
			case FLOAT_1:ret= Float.valueOf(1);break;
			case FLOAT_255:ret= Float.valueOf(is.readUnsignedByte());break;
			case FLOAT_SHORT:ret=  Float.valueOf(is.readShort());break;
			case FLOAT_FULL:ret= Float.valueOf(is.readFloat());break;
			case DOUBLE_MINUS_1:ret= Double.valueOf(-1);break;
			case DOUBLE_0:ret= Double.valueOf(0);break;
			case DOUBLE_1:ret= Double.valueOf(1);break;
			case DOUBLE_255:ret= Double.valueOf(is.readUnsignedByte());break;
			case DOUBLE_SHORT:ret= Double.valueOf(is.readShort());break;
			case DOUBLE_FULL:ret= Double.valueOf(is.readDouble());break;
                        case BIGINTEGER: ret = new BigInteger(deserializeArrayByteInt(is));break;
                        case BIGDECIMAL: ret = new BigDecimal(new BigInteger(deserializeArrayByteInt(is)),LongPacker.unpackInt(is));break;
			case STRING:ret= deserializeString(is);break;
			case STRING_EMPTY:ret= EMPTY_STRING;break;
			case ARRAYLIST:ret= deserializeArrayList(is);break;
			case ARRAYLIST_PACKED_LONG:ret= deserializeArrayListPackedLong(is);break;
			case ARRAY_OBJECT:ret= deserializeArrayObject(is);break;
			case ARRAY_OBJECT_PACKED_LONG:ret= deserializeArrayObjectPackedLong(is);break;
			case LINKEDLIST:ret= deserializeLinkedList(is);break;
			case TREESET:ret= deserializeTreeSet(is);break;
			case HASHSET:ret= deserializeHashSet(is);break;
			case LINKEDHASHSET:ret= deserializeLinkedHashSet(is);break;
			case VECTOR:ret= deserializeVector(is);break;
			case TREEMAP:ret= deserializeTreeMap(is);break;
			case HASHMAP:ret= deserializeHashMap(is);break;
			case LINKEDHASHMAP:ret= deserializeLinkedHashMap(is);break;
			case HASHTABLE:ret= deserializeHashtable(is);break;
			case PROPERTIES:ret= deserializeProperties(is);break;
			case CLASS:ret = deserializeClass(is);break;
                        case DATE:ret = new Date(is.readLong());break;


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
			case ARRAY_BYTE_INT: ret= deserializeArrayByteInt(is);break;
            case BTREE: ret = BTree.readExternal(is,this); break;
			case BPAGE_LEAF: throw new InternalError("BPage header, wrong serializer used");
			case BPAGE_NONLEAF: throw new InternalError("BPage header, wrong serializer used");
			case JAVA_SERIALIZATION: throw new InternalError("Wrong header, data were propably serialized with OutputStream, not with JDBM serialization");

			case -1: throw new EOFException();

			default: throw new InternalError("Unknown serialization header: "+head);
    	}

    	if(DEBUGSTORE && is.readInt()!=DEBUGSTORE_DUMMY_END){
    		throw new InternalError("Wrong offset '"+ret+ "' - "+ret.getClass());
    	}

        objectStack.add(ret); //TODO there is serious problem with order in which objects are added


    	return ret;
	}


	private Class deserializeClass(DataInput is) throws IOException, ClassNotFoundException {
		String className = (String) deserialize(is);
		Class cls = Class.forName(className);
		return cls;
	}


	private byte[] deserializeArrayByteInt(DataInput is) throws IOException {
		int size = LongPacker.unpackInt(is);
		byte[] b = new byte[size];
		is.readFully(b);
		return b;
	}




	private long[] deserializeArrayLongL(DataInput is) throws IOException {
		int size = LongPacker.unpackInt(is);
		long[] ret = new long[size];
		for(int i=0;i<size;i++)
			ret[i] = is.readLong();
		return ret;
	}


	private long[] deserializeArrayLongI(DataInput is) throws IOException {
		int size = LongPacker.unpackInt(is);
		long[] ret = new long[size];
		for(int i=0;i<size;i++)
			ret[i] = is.readInt();
		return ret;
	}


	private long[] deserializeArrayLongS(DataInput is) throws IOException {
		int size = LongPacker.unpackInt(is);
		long[] ret = new long[size];
		for(int i=0;i<size;i++)
			ret[i] = is.readShort();
		return ret;
	}


	private long[] deserializeArrayLongB(DataInput is) throws IOException {
		int size = LongPacker.unpackInt(is);
		long[] ret = new long[size];
		for(int i=0;i<size;i++){
			ret[i] = is.readUnsignedByte();
			if(ret[i] <0)
	    	    throw new EOFException();
		}
		return ret;
	}


	private int[] deserializeArrayIntIInt(DataInput is) throws IOException {
		int size = LongPacker.unpackInt(is);
		int[] ret = new int[size];
		for(int i=0;i<size;i++)
			ret[i] = is.readInt();
		return ret;
	}


	private int[] deserializeArrayIntSInt(DataInput is) throws IOException {
		int size = LongPacker.unpackInt(is);
		int[] ret = new int[size];
		for(int i=0;i<size;i++)
			ret[i] = is.readShort();
		return ret;
	}



	private int[] deserializeArrayIntBInt(DataInput is) throws IOException {
		int size = LongPacker.unpackInt(is);
		int[] ret = new int[size];
		for(int i=0;i<size;i++){
			ret[i] = is.readUnsignedByte();
			if(ret[i] <0)
	    	    throw new EOFException();
		}
		return ret;	}


	private int[] deserializeArrayIntPack(DataInput is) throws IOException {
		int size = LongPacker.unpackInt(is);
    	if (size < 0)
    	    throw new EOFException();

		int[] ret = new int[size];
		for(int i=0;i<size;i++){
			ret[i] = LongPacker.unpackInt(is);
		}
		return ret;
	}

	private long[] deserializeArrayLongPack(DataInput is) throws IOException {
		int size = LongPacker.unpackInt(is);
    	if (size < 0)
    	    throw new EOFException();

		long[] ret = new long[size];
		for(int i=0;i<size;i++){
			ret[i] = LongPacker.unpackLong(is);
		}
		return ret;
	}

	private int[] deserializeArrayIntB255(DataInput is) throws IOException {
		int size = is.readUnsignedByte();
    	if (size < 0)
    	    throw new EOFException();

		int[] ret = new int[size];
		for(int i=0;i<size;i++){
			ret[i] = is.readUnsignedByte();
			if(ret[i] <0)
	    	    throw new EOFException();
		}
		return ret;	}



	private Object[] deserializeArrayObject(DataInput is) throws IOException, ClassNotFoundException {
		int size =LongPacker.unpackInt(is);
		Object[] s = new Object[size];
		for(int i = 0; i<size;i++)
			s[i] = deserialize(is);
		return s;
	}

	private Object[] deserializeArrayObjectPackedLong(DataInput is) throws IOException, ClassNotFoundException {
		int size = is.readUnsignedByte();
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



	private ArrayList<Object> deserializeArrayList(DataInput is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);
		ArrayList<Object> s = new ArrayList<Object>(size);
		for(int i = 0; i<size;i++)
			s.add(deserialize(is));
		return s;
	}

	private ArrayList<Object> deserializeArrayListPackedLong(DataInput is) throws IOException, ClassNotFoundException {
		int size = is.readUnsignedByte();
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


	private LinkedList<Object> deserializeLinkedList(DataInput is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);
		LinkedList<Object> s = new LinkedList<Object>();
		for(int i = 0; i<size;i++)
			s.add(deserialize(is));
		return s;
	}


	private Vector<Object> deserializeVector(DataInput is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);
		Vector<Object> s = new Vector<Object>(size);
		for(int i = 0; i<size;i++)
			s.add(deserialize(is));
		return s;
	}


	private HashSet<Object> deserializeHashSet(DataInput is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);
		HashSet<Object> s = new HashSet<Object>(size);
		for(int i = 0; i<size;i++)
			s.add(deserialize(is));
		return s;
	}


	private LinkedHashSet<Object> deserializeLinkedHashSet(DataInput is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);
		LinkedHashSet<Object> s = new LinkedHashSet<Object>(size);
		for(int i = 0; i<size;i++)
			s.add(deserialize(is));
		return s;
	}



	private TreeSet<Object> deserializeTreeSet(DataInput is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);
		TreeSet<Object> s = new TreeSet<Object>();
		Comparator comparator = (Comparator) deserialize(is);
		if(comparator!=null)
			s = new TreeSet<Object>(comparator);

		for(int i = 0; i<size;i++)
			s.add(deserialize(is));
		return s;
	}



	private TreeMap<Object,Object> deserializeTreeMap(DataInput is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);

		TreeMap<Object,Object> s = new TreeMap<Object,Object>();
		Comparator comparator = (Comparator) deserialize(is);
		if(comparator!=null)
			s = new TreeMap<Object,Object>(comparator);
		for(int i = 0; i<size;i++)
			s.put(deserialize(is),deserialize(is));
		return s;
	}



	private HashMap<Object,Object> deserializeHashMap(DataInput is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);

		HashMap<Object,Object> s = new HashMap<Object,Object>(size);
		for(int i = 0; i<size;i++)
			s.put(deserialize(is),deserialize(is));
		return s;
	}




	private LinkedHashMap<Object,Object> deserializeLinkedHashMap(DataInput is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);

		LinkedHashMap<Object,Object> s = new LinkedHashMap<Object,Object>(size);
		for(int i = 0; i<size;i++)
			s.put(deserialize(is),deserialize(is));
		return s;
	}



	private  Hashtable<Object,Object> deserializeHashtable(DataInput is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);

		Hashtable<Object,Object> s = new Hashtable<Object,Object>(size);
		for(int i = 0; i<size;i++)
			s.put(deserialize(is),deserialize(is));
		return s;
	}



	private  Properties deserializeProperties(DataInput is) throws IOException, ClassNotFoundException {
		int size = LongPacker.unpackInt(is);

		Properties s = new Properties();
		for(int i = 0; i<size;i++)
			s.put(deserialize(is),deserialize(is));
		return s;
	}

}
