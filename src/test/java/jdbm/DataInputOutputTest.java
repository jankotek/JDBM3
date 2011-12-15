package jdbm;

import junit.framework.TestCase;

import java.io.IOException;

public class DataInputOutputTest extends TestCase {
    
    final DataInputOutput d = new DataInputOutput();
    
    public void testInt() throws IOException {
        int i = 123129049;
        d.writeInt(i);
        d.reset();
        assertEquals(i, d.readInt());
    }

    public void testLong() throws IOException {
        long i = 1231290495545446485L;
        d.writeLong(i);
        d.reset();
        assertEquals(i, d.readLong());
    }
    
    
    public void testBooelean() throws IOException {        
        d.writeBoolean(true);
        d.reset();
        assertEquals(true, d.readBoolean());
        d.reset();
        d.writeBoolean(false);
        d.reset();
        assertEquals(false, d.readBoolean());
        
    }
    

    public void testByte() throws IOException {
        
        for(int i = Byte.MIN_VALUE; i<=Byte.MAX_VALUE;i++){
            d.writeByte(i);
            d.reset();
            assertEquals(i, d.readByte());
            d.reset();
        }
    }


    public void testUnsignedByte() throws IOException {

        for(int i =0; i<=255;i++){
            d.write(i);
            d.reset();
            assertEquals(i, d.readUnsignedByte());
            d.reset();
        }
    }


    public void testLongPacker() throws IOException {

        for(int i =0; i<1e7;i++){
            LongPacker.packInt(d,i);
            d.reset();
            assertEquals(i, LongPacker.unpackInt(d));
            d.reset();
        }
    }



}


