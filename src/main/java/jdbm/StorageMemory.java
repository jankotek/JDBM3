package jdbm;

import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Storage which keeps all data in memory.
 * Data are lost after storage is closed.
 */
class StorageMemory implements Storage{

    private ArrayList<byte[]> pages = new ArrayList<byte[]>();


    public void read(long pageNumber, byte[] data) throws IOException {
        if(data.length!=RecordFile.BLOCK_SIZE) throw new IllegalArgumentException();
        if(pages.size()<=pageNumber || pages.get((int) pageNumber)==null){
            //out of bounds, so just return empty data
            System.arraycopy(RecordFile.CLEAN_DATA,0 ,data,0,data.length);
            return;
        }

        byte[] data2 = pages.get((int) pageNumber);
        System.arraycopy(data2,0,data,0,data.length);
    }

    public void write(long pageNumber, byte[] data) throws IOException {
        if(data.length!=RecordFile.BLOCK_SIZE) throw new IllegalArgumentException();

        byte[] data2 = new byte[data.length];
        System.arraycopy(data,0,data2,0,data.length);

        pages.ensureCapacity((int) (pageNumber+1));
        pages.set((int) pageNumber,data2);
    }

    public void sync() throws IOException {
    }




    public void forceClose() throws IOException {
        pages = null;
    }

    private ByteArrayOutputStream transLog;

    public DataInputStream readTransactionLog() {
        if(transLog == null)
            return null;
        return new DataInputStream(new ByteArrayInputStream(transLog.toByteArray()));
    }

    public void deleteTransactionLog() {
        transLog = null;
    }

    public DataOutputStream openTransactionLog() throws IOException {
        if(transLog == null)
            transLog = new ByteArrayOutputStream();
        return new DataOutputStream(transLog);
    }
}
