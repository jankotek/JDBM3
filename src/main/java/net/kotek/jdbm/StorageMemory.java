package net.kotek.jdbm;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Storage which keeps all data in memory.
 * Data are lost after storage is closed.
 */
class StorageMemory implements Storage {

    private ArrayList<byte[]> pages = new ArrayList<byte[]>();
    private ArrayList<byte[]> pagesLogical = new ArrayList<byte[]>();


    public ByteBuffer read(long pageNumber) throws IOException {
        
        ArrayList<byte[]> p = pageNumber<0 ? pagesLogical : pages;
        pageNumber = Math.abs(pageNumber);

        if (p.size() <=  pageNumber || p.get((int) pageNumber) == null) {
            //out of bounds, so just return empty data
            return ByteBuffer.wrap(RecordFile.CLEAN_DATA).asReadOnlyBuffer();
        }

        byte[] data = p.get((int) pageNumber);
        return ByteBuffer.wrap(data).asReadOnlyBuffer();
    }

    public void write(long pageNumber, ByteBuffer data) throws IOException {
        if (data.capacity() != BLOCK_SIZE) throw new IllegalArgumentException();

        ArrayList<byte[]> p = pageNumber<0 ? pagesLogical : pages;
        pageNumber = Math.abs(pageNumber);

        byte[] data2 = new byte[BLOCK_SIZE];
        System.arraycopy(data.array(), 0, data2, 0, BLOCK_SIZE);

        p.ensureCapacity((int) (pageNumber + 1));
        while(p.size()<=pageNumber+1)p.add(null);
        p.set((int) pageNumber, data2);
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
        return new DataInputStream(new ByteArrayInputStream(transLog.toByteArray()));
    }

    public void deleteTransactionLog() {
        transLog = null;
    }

    public DataOutputStream openTransactionLog() throws IOException {
        if (transLog == null)
            transLog = new ByteArrayOutputStream();
        return new DataOutputStream(transLog);
    }

    public boolean isReadonly() {
        return false;
    }
}
