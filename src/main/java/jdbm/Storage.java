package jdbm;

import java.io.*;
import java.nio.ByteBuffer;

/**
 *
 */
interface Storage {

    /**
     * the lenght of single block
     */
    int BLOCK_SIZE = 2048;

    void write(long pageNumber, ByteBuffer data) throws IOException;

    ByteBuffer read(long pageNumber) throws IOException;

    void forceClose() throws IOException;

    boolean isReadonly();

    DataInputStream readTransactionLog();

    void deleteTransactionLog();

    void sync() throws IOException;

    DataOutputStream openTransactionLog() throws IOException;
}
