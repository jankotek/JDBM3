package jdbm;

import java.io.*;

/**
 *
 */
interface Storage {

    /** the lenght of single block */
    int BLOCK_SIZE =2048;

    void write(long pageNumber, byte[] data) throws IOException;
    void read(long pageNumber, byte[] data) throws IOException;

    void forceClose() throws IOException;

    DataInputStream readTransactionLog();

    void deleteTransactionLog();

    void sync() throws IOException;

    DataOutputStream openTransactionLog() throws  IOException;
}
