package net.kotek.jdbm;

import sun.misc.Cleaner;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.IdentityHashMap;

/**
 * Disk storage which uses mapped buffers
 */
public class StorageDiskMapped implements Storage {


    /**
     * maximal number of pages in single file
     */
    private final static long PAGES_PER_FILE = BLOCK_SIZE * 1024 * 512 ;


    private ArrayList<FileChannel> channels = new ArrayList<FileChannel>();
    private IdentityHashMap<FileChannel, MappedByteBuffer> buffers = new IdentityHashMap<FileChannel, MappedByteBuffer>();

    private String fileName;
    private boolean transactionsDisabled;
    private boolean readonly;


    public StorageDiskMapped(String fileName, boolean readonly, boolean transactionsDisabled) throws IOException {
        this.fileName = fileName;
        this.transactionsDisabled = transactionsDisabled;
        this.readonly = readonly;
        //make sure first file can be opened
        //lock it
        try {
            getChannel(0).lock();
        } catch (IOException e) {
            throw new IOException("Could not lock DB file: " + fileName, e);
        } catch (OverlappingFileLockException e) {
            throw new IOException("Could not lock DB file: " + fileName, e);
        }

    }

    private FileChannel getChannel(long pageNumber) throws IOException {
        int fileNumber = (int) (pageNumber/PAGES_PER_FILE );

        //increase capacity of array lists if needed
        for (int i = channels.size(); i <= fileNumber; i++) {
            channels.add(null);
        }

        FileChannel ret = channels.get(fileNumber);
        if (ret == null) {
            String name = fileName + "." + fileNumber;
            ret = new RandomAccessFile(name, "rw").getChannel();
            channels.set(fileNumber, ret);
            buffers.put(ret, ret.map(FileChannel.MapMode.READ_WRITE, 0, ret.size()));
        }
        return ret;
    }
    

    public void write(long pageNumber, ByteBuffer data) throws IOException {
        if(transactionsDisabled && data.isDirect()){
            //if transactions are disabled and this buffer is direct,
            //changes written into buffer are directly reflected in file.
            //so there is no need to write buffer second time
            return;
        }
        
        FileChannel f = getChannel(pageNumber);
        int offsetInFile = (int) ((pageNumber % PAGES_PER_FILE)*BLOCK_SIZE);
        MappedByteBuffer b = buffers.get(f);
        if( b.limit()<=offsetInFile){

            //remapping buffer for each newly added page would be slow,
            //so allocate new size in chunks
            int increment = Math.min(BLOCK_SIZE * 1000,offsetInFile/10);
            increment  -= increment%BLOCK_SIZE;

            long newFileSize = offsetInFile+BLOCK_SIZE + increment;
            newFileSize = Math.min(PAGES_PER_FILE * BLOCK_SIZE, newFileSize);

            //expand file size
            f.position(newFileSize - 1);
            f.write(ByteBuffer.allocate(1));
            //unmap old buffer
            unmapBuffer(b);
            //remap buffer
            b = f.map(FileChannel.MapMode.READ_WRITE, 0,newFileSize);
            buffers.put(f, b);
        }

        //write into buffer
        b.position(offsetInFile);
        data.rewind();
        b.put(data);
    }

    private void unmapBuffer(MappedByteBuffer b) {
        if(b!=null){
            Cleaner cleaner = ((sun.nio.ch.DirectBuffer) b).cleaner();
            if(cleaner!=null)
                cleaner.clean();
        }
    }

    public ByteBuffer read(long pageNumber) throws IOException {
        FileChannel f = getChannel(pageNumber);
        int offsetInFile = (int) ((pageNumber % PAGES_PER_FILE)*BLOCK_SIZE);
        MappedByteBuffer b = buffers.get(f);
        
        if(b == null){ //not mapped yet
            b = f.map(FileChannel.MapMode.READ_WRITE, 0, f.size());
        }
        
        //check buffers size
        if(b.limit()<=offsetInFile){
                //file is smaller, return empty data
                return ByteBuffer.wrap(RecordFile.CLEAN_DATA).asReadOnlyBuffer();
            }

        b.position(offsetInFile);
        ByteBuffer ret = b.slice();
        ret.limit(BLOCK_SIZE);
        if(!transactionsDisabled||readonly){
            // changes written into buffer will be directly written into file
            // so we need to protect buffer from modifications
            ret = ret.asReadOnlyBuffer();
        }
        return ret;
    }

    public void forceClose() throws IOException {
        for(FileChannel f: channels){
            if(f==null) continue;
            f.close();
            unmapBuffer(buffers.get(f));
        }
        channels = null;
        buffers = null;
    }

    public void sync() throws IOException {
        for(MappedByteBuffer b: buffers.values()){
            b.force();
        }
    }


    public DataOutputStream openTransactionLog() throws IOException {
        String logName = fileName + StorageDisk.transaction_log_file_extension;
        final FileOutputStream fileOut = new FileOutputStream(logName);
        return new DataOutputStream(new BufferedOutputStream(fileOut)) {

            //default implementation of flush on FileOutputStream does nothing,
            //so we use little workaround to make sure that data were really flushed
            public void flush() throws IOException {
                super.flush();
                fileOut.flush();
                fileOut.getFD().sync();
            }
        };
    }


    public DataInputStream readTransactionLog() {

        File logFile = new File(fileName + StorageDisk.transaction_log_file_extension);
        if (!logFile.exists())
            return null;
        if (logFile.length() == 0) {
            logFile.delete();
            return null;
        }

        DataInputStream ois = null;
        try {
            ois = new DataInputStream(new BufferedInputStream(new FileInputStream(logFile)));
        } catch (FileNotFoundException e) {
            //file should exists, we check for its presents just a miliseconds yearlier, anyway move on
            return null;
        }

        try {
            if (ois.readShort() != Magic.LOGFILE_HEADER)
                throw new Error("Bad magic on log file");
        } catch (IOException e) {
            // corrupted/empty logfile
            logFile.delete();
            return null;
        }
        return ois;
    }

    public void deleteTransactionLog() {
        File logFile = new File(fileName + StorageDisk.transaction_log_file_extension);
        if (logFile.exists())
            logFile.delete();
    }

    public boolean isReadonly() {
        return readonly;
    }


}
