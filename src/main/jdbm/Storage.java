package jdbm;

import java.io.*;

/**
 *
 */
public interface Storage {


    void read(long offset, byte[] data, int blockSize) throws IOException;

    void forceClose() throws IOException;

    void write(long offset, byte[] data) throws IOException;

    DataInputStream readTransactionLog();

    void deleteTransactionLog();

    void sync() throws IOException;

    DataOutputStream openTransactionLog() throws FileNotFoundException, IOException;
}
