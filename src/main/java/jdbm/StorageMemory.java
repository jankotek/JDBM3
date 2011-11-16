package jdbm;

import java.io.*;
import java.util.Arrays;

/**
 * Storage which keeps all data in memory.
 * Data are lost after storage is closed.
 */
//TODO StorageMemory is currently limited to 2GB
class StorageMemory implements Storage{

    private byte[] store = new byte[RecordFile.BLOCK_SIZE * 16];

    public void read(long offset, byte[] data, int blockSize) throws IOException {
        if(store.length<=offset){
            //out of bounds, so just return empty data
            System.arraycopy(RecordFile.CLEAN_DATA,0 ,data,0,blockSize);
            return;
        }
        ensureSize(offset+blockSize);
        System.arraycopy(store, (int) offset,data,0,blockSize);
    }

    public void write(long offset, byte[] data) throws IOException {
        ensureSize(offset+data.length);
        System.arraycopy(data,0,store, (int) offset,data.length);
    }

    private void ensureSize(long size){
        if(size>=Integer.MAX_VALUE)
            throw new Error("Memory storage does not supports store over 2GB");

        if(store.length<size){
            long newSize = store.length;
            while(newSize<size){
                newSize = Math.min(Integer.MAX_VALUE, size*2);
            }
            //grow
            store = Arrays.copyOf(store, (int) newSize);
        }

    }


    public void sync() throws IOException {
    }


    public void forceClose() throws IOException {
        store = null;
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
