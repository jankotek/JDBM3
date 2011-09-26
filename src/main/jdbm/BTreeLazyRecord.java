package jdbm;

import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;

/**
 * An record lazly loaded from store.
 * TThis is used in BTree/HTree to store big records outside of index
 */
class BTreeLazyRecord<E> {
    
    private E value = null;
    private RecordManager recman;
    private Serializer<E> serializer;
    final long recid;

    BTreeLazyRecord(RecordManager recman, long recid, Serializer<E> serializer){
        this.recman = recman;
        this.recid = recid;
        this.serializer = serializer;
    }


    E get(){
        if(value!=null) return value;
        try {
            value = recman.fetch(recid,serializer);
        } catch (IOException e) {
            throw new IOError(e);
        }
        return value;
    }

    void delete(){
        try {
            recman.delete(recid);
        } catch (IOException e) {
            throw new IOError(e);
        }
        value = null;
        serializer = null;
        recman = null;
    }

    /**
     * Serialier used to insert already serialized data into store
     */
    static final Serializer FAKE_SERIALIZER = new Serializer(){

        public void serialize(SerializerOutput out, Object obj) throws IOException {
            byte[] data = (byte[]) obj;
            out.write(data);
        }

        public Object deserialize(SerializerInput in) throws IOException, ClassNotFoundException {
            throw new UnsupportedOperationException();
        }
    };


    static byte[] readByteArray( DataInputStream in )
        throws IOException
    {
        int len = LongPacker.unpackInt(in);
        if ( len == 0 ) {
            return null;
        }
        byte[] buf = new byte[ len-1 ];
        in.readFully( buf );
        return buf;
    }

    static void writeByteArray(SerializerOutput out, byte[] buf)
        throws IOException
    {
        if ( buf == null ) {
            out.write( 0 );
        } else {
        	LongPacker.packInt( out, buf.length+1);
            out.write( buf );
        }
    }


    /**
     * if value in tree is serialized in more bytes, it is stored as separate record outside of tree
     * This value must be always smaller than 250
     */
    static final int MAX_INTREE_RECORD_SIZE= 64;
    static{
        if(MAX_INTREE_RECORD_SIZE>250) throw new Error();
    }
    static final int NULL = 255;
    static final int LAZY_RECORD = 254;

}
