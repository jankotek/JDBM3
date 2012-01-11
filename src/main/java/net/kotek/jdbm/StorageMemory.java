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


    public ByteBuffer read(long pageNumber) throws IOException {
        ByteBuffer data = ByteBuffer.allocate(BLOCK_SIZE);
        if (pages.size() <= pageNumber || pages.get((int) pageNumber) == null) {
            //out of bounds, so just return empty data
            System.arraycopy(RecordFile.CLEAN_DATA, 0, data.array(), 0, data.capacity());
            return data;
        }

        byte[] data2 = pages.get((int) pageNumber);
        System.arraycopy(data2, 0, data.array(), 0, data.capacity());
        return data;
    }

    public void write(long pageNumber, ByteBuffer data) throws IOException {
        if (data.capacity() != BLOCK_SIZE) throw new IllegalArgumentException();

        byte[] data2 = new byte[data.capacity()];
        System.arraycopy(data.array(), 0, data2, 0, data.capacity());

        pages.ensureCapacity((int) (pageNumber + 1));
        while(pages.size()<=pageNumber+1)pages.add(null);
        pages.set((int) pageNumber, data2);
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
