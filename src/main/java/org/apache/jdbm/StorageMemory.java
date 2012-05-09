package org.apache.jdbm;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Storage which keeps all data in memory.
 * Data are lost after storage is closed.
 */
class StorageMemory implements Storage {

    private LongHashMap<byte[]> pages = new LongHashMap<byte[]>();
    private boolean transactionsDisabled;

    StorageMemory(boolean transactionsDisabled){
        this.transactionsDisabled = transactionsDisabled;
    }


    public ByteBuffer read(long pageNumber) throws IOException {

        byte[] data = pages.get(pageNumber);
        if (data == null) {
            //out of bounds, so just return empty data
            return ByteBuffer.wrap(PageFile.CLEAN_DATA).asReadOnlyBuffer();
        }else{
            ByteBuffer b = ByteBuffer.wrap(data);
            if(!transactionsDisabled)
                return b.asReadOnlyBuffer();
            else
                return b;
        }


    }

    public void write(long pageNumber, ByteBuffer data) throws IOException {
        if (data.capacity() != PAGE_SIZE) throw new IllegalArgumentException();

        byte[] b = pages.get(pageNumber);

        if(transactionsDisabled && data.hasArray() && data.array() == b){
            //already putted directly into array
            return;
        }

        
        if(b == null)
            b = new byte[PAGE_SIZE];
        
        data.position(0);
        data.get(b,0, PAGE_SIZE);
        pages.put(pageNumber,b);
    }

    public void sync() throws IOException {
    }


    public void forceClose() throws IOException {
        pages = null;
    }

    private ByteArrayOutputStream transLog;

    public DataInputStream readTransactionLog() {
        if (transLog == null)
            return null;
        DataInputStream ret =  new DataInputStream(
                new ByteArrayInputStream(transLog.toByteArray()));
        //read stream header
        try {
            ret.readShort();
        } catch (IOException e) {
            throw new IOError(e);
        }
        return ret;
    }

    public void deleteTransactionLog() {
        transLog = null;
    }

    public DataOutputStream openTransactionLog() throws IOException {
        if (transLog == null)
            transLog = new ByteArrayOutputStream();
        return new DataOutputStream(transLog);
    }

    public void deleteAllFiles() throws IOException {
    }

    public boolean isReadonly() {
        return false;
    }
}
