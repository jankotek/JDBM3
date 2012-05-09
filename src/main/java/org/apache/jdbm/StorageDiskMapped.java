package org.apache.jdbm;

import sun.misc.Cleaner;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

/**
 * Disk storage which uses mapped buffers
 */
class StorageDiskMapped implements Storage {

    static final String IDR = ".i";

    static final String DBR = ".d";



    /**
     * Maximal number of pages in single file.
     * Calculated so that each file will have 1 GB
     */
    final static long PAGES_PER_FILE = (1024*1024*1024)>>>Storage.PAGE_SIZE_SHIFT;



    private ArrayList<FileChannel> channels = new ArrayList<FileChannel>();
    private ArrayList<FileChannel> channelsTranslation = new ArrayList<FileChannel>();
    private IdentityHashMap<FileChannel, MappedByteBuffer> buffers = new IdentityHashMap<FileChannel, MappedByteBuffer>();

    private String fileName;
    private boolean transactionsDisabled;
    private boolean readonly;
    private boolean lockingDisabled;


    public StorageDiskMapped(String fileName, boolean readonly, boolean transactionsDisabled, boolean lockingDisabled) throws IOException {
        this.fileName = fileName;
        this.transactionsDisabled = transactionsDisabled;
        this.readonly = readonly;
        this.lockingDisabled = lockingDisabled;
        //make sure first file can be opened
        //lock it
        try {
            if(!lockingDisabled)
                getChannel(0).lock();
        } catch (IOException e) {
            throw new IOException("Could not lock DB file: " + fileName, e);
        } catch (OverlappingFileLockException e) {
            throw new IOException("Could not lock DB file: " + fileName, e);
        }

    }

    private FileChannel getChannel(long pageNumber) throws IOException {
        int fileNumber = (int) (Math.abs(pageNumber)/PAGES_PER_FILE );

        List<FileChannel> c = pageNumber>=0 ? channels : channelsTranslation;

        //increase capacity of array lists if needed
        for (int i = c.size(); i <= fileNumber; i++) {
            c.add(null);
        }

        FileChannel ret = c.get(fileNumber);
        if (ret == null) {
            String name = makeFileName(fileName, pageNumber, fileNumber);
            ret = new RandomAccessFile(name, "rw").getChannel();
            c.set(fileNumber, ret);
            buffers.put(ret, ret.map(FileChannel.MapMode.READ_WRITE, 0, ret.size()));
        }
        return ret;
    }

    static String makeFileName(String fileName, long pageNumber, int fileNumber) {
        return fileName + (pageNumber>=0 ? DBR : IDR) + "." + fileNumber;
    }


    public void write(long pageNumber, ByteBuffer data) throws IOException {
        if(transactionsDisabled && data.isDirect()){
            //if transactions are disabled and this buffer is direct,
            //changes written into buffer are directly reflected in file.
            //so there is no need to write buffer second time
            return;
        }
        
        FileChannel f = getChannel(pageNumber);
        int offsetInFile = (int) ((Math.abs(pageNumber) % PAGES_PER_FILE)* PAGE_SIZE);
        MappedByteBuffer b = buffers.get(f);
        if( b.limit()<=offsetInFile){

            //remapping buffer for each newly added page would be slow,
            //so allocate new size in chunks
            int increment = Math.min(PAGE_SIZE * 1024,offsetInFile/10);
            increment  -= increment% PAGE_SIZE;

            long newFileSize = offsetInFile+ PAGE_SIZE + increment;
            newFileSize = Math.min(PAGES_PER_FILE * PAGE_SIZE, newFileSize);

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
        int offsetInFile = (int) ((Math.abs(pageNumber) % PAGES_PER_FILE)* PAGE_SIZE);
        MappedByteBuffer b = buffers.get(f);
        
        if(b == null){ //not mapped yet
            b = f.map(FileChannel.MapMode.READ_WRITE, 0, f.size());
        }
        
        //check buffers size
        if(b.limit()<=offsetInFile){
                //file is smaller, return empty data
                return ByteBuffer.wrap(PageFile.CLEAN_DATA).asReadOnlyBuffer();
            }

        b.position(offsetInFile);
        ByteBuffer ret = b.slice();
        ret.limit(PAGE_SIZE);
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
        for(FileChannel f: channelsTranslation){
            if(f==null) continue;
            f.close();
            unmapBuffer(buffers.get(f));
        }

        channels = null;
        channelsTranslation = null;
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

    public void deleteAllFiles() throws IOException {
        deleteTransactionLog();
        deleteFiles(fileName);
    }

    static void deleteFiles(String fileName) {
        for(int i = 0; true; i++){
            String name = makeFileName(fileName,+1, i);
            File f =new File(name);
            boolean exists = f.exists();
            if(exists && !f.delete()) f.deleteOnExit();
            if(!exists) break;
        }
        for(int i = 0; true; i++){
            String name = makeFileName(fileName,-1, i);
            File f =new File(name);
            boolean exists = f.exists();
            if(exists && !f.delete()) f.deleteOnExit();
            if(!exists) break;
        }
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
