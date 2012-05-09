package org.apache.jdbm;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * A read-only storage which reads data from compressed zip archive.
 * <p/>
 * To improve performance with compressed archives
 * each page is stored in separate file (zip archive entry).
 */
class StorageZip implements Storage {


    private String zip;
    private String zip2;
    private ZipFile z;

    StorageZip(String zipFile) throws IOException {
        zip = zipFile;
        z = new ZipFile(zip);
        zip2 = "db";
    }

    public void write(long pageNumber, ByteBuffer data) throws IOException {
        throw new UnsupportedOperationException("readonly");
    }

    public ByteBuffer read(long pageNumber) throws IOException {
        ByteBuffer data = ByteBuffer.allocate(PAGE_SIZE);

        ZipEntry e = z.getEntry(zip2 + pageNumber);
        if(e == null)
            return ByteBuffer.wrap(PageFile.CLEAN_DATA).asReadOnlyBuffer();

        InputStream i = z.getInputStream(e);
        new DataInputStream(i).readFully(data.array());
        i.close();
        return data;
    }

    public void forceClose() throws IOException {
        z.close();
        z = null;
    }

    public DataInputStream readTransactionLog() {
        throw new UnsupportedOperationException("readonly");
    }

    public void deleteTransactionLog() {
        throw new UnsupportedOperationException("readonly");
    }

    public void sync() throws IOException {
        throw new UnsupportedOperationException("readonly");
    }

    public DataOutputStream openTransactionLog() throws IOException {
        throw new UnsupportedOperationException("readonly");
    }

    public void deleteAllFiles() throws IOException {
    }

    public boolean isReadonly() {
        return true;
    }
}
