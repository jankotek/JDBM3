package net.kotek.jdbm;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Allows using different key-value database backend.
 *
 * NOTE this interface may change before stable release
 */
public interface RecordManager {
    
    long insert(byte[] rec, int pos, int length) throws IOException;
    void update(long recid, byte[] rec, int pos, int length) throws IOException;
    void delete(long recid) throws IOException;
    boolean supportsTransaction();
    void commit() throws IOException;
    void rollback() throws IOException;

    void close() throws IOException;

    boolean fetch(long recid, DataOutput buf) throws IOException;
    
    String calculateStatistics() throws IOException;

    void defrag(boolean fullDefrag) throws IOException;

    boolean needsAutoCommit();

    long getRoot(int rootId) throws IOException;

    void setRoot(int rootId, long rootVal) throws IOException;

    byte[] fetchRaw(long recid) throws IOException;

    void forceInsert(long recid, byte[] data) throws IOException;
}
