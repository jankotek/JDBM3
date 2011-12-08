package jdbm;

import java.io.*;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;

/**
 * Storage which used files on disk to store data
 */
class StorageDisk implements Storage{

    /** maximal file size not rounded to block size */
    private final static long _FILESIZE = 1000000000l;
    /** maximal file size rounded to block size */
    private final long MAX_FILE_SIZE =  _FILESIZE - _FILESIZE % BLOCK_SIZE;


    private ArrayList<RandomAccessFile> rafs = new ArrayList<RandomAccessFile>();

    private String fileName;

    public StorageDisk(String fileName) throws IOException {
        this.fileName = fileName;
        //make sure first file can be opened
        //lock it
        try{
            getRaf(0).getChannel().tryLock();
        }catch(IOException e){
            throw new IOException("Could not lock DB file: "+fileName,e);
        }catch(OverlappingFileLockException e){
            throw new IOException("Could not lock DB file: "+fileName,e);
        }

    }

    RandomAccessFile getRaf(long offset) throws IOException {
        int fileNumber = (int) (offset/MAX_FILE_SIZE);

        //increase capacity of array lists if needed
        for(int i = rafs.size();i<=fileNumber;i++){
            rafs.add(null);
        }

        RandomAccessFile ret = rafs.get(fileNumber);
        if(ret == null){
            String name = fileName+"."+fileNumber;
            ret = new RandomAccessFile(name, "rw");
            rafs.set(fileNumber, ret);
        }
        return ret;
    }

    /**
     *  Synchronizes the file.
     */
    public void sync() throws IOException {
        for(RandomAccessFile file:rafs)
            if(file!=null)
                file.getFD().sync();
    }


    public void write(long pageNumber, byte[] data) throws IOException {
        if(data.length!= BLOCK_SIZE) throw new IllegalArgumentException();
        long offset = pageNumber * BLOCK_SIZE;

        RandomAccessFile file = getRaf(offset);
        file.seek(offset % MAX_FILE_SIZE);
        file.write(data);
    }

    public void forceClose() throws IOException {
        for(RandomAccessFile f :rafs){
            if(f!=null)
                f.close();
        }
        rafs = null;
    }

    public void read(long pageNumber, byte[] buffer) throws IOException {
        if(buffer.length!= BLOCK_SIZE) throw new IllegalArgumentException();
        long offset = pageNumber * BLOCK_SIZE;

        RandomAccessFile file = getRaf(offset);
        file.seek(offset%MAX_FILE_SIZE);
        int remaining = buffer.length;
        int pos = 0;
        while (remaining > 0) {
            int read = file.read(buffer, pos, remaining);
            if (read == -1) {
                System.arraycopy(RecordFile.CLEAN_DATA, 0, buffer, pos, remaining);
                break;
            }
            remaining -= read;
            pos += read;
        }
    }


    static final String transaction_log_file_extension = ".t";


    public DataOutputStream openTransactionLog() throws IOException {
        String logName = fileName + transaction_log_file_extension;
        final FileOutputStream fileOut = new FileOutputStream(logName);
        return new DataOutputStream(new BufferedOutputStream(fileOut)){

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

        File logFile = new File(fileName + transaction_log_file_extension);
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
        File logFile = new File(fileName + transaction_log_file_extension);
        if(logFile.exists())
            logFile.delete();
    }

    public boolean isReadonly(){return false;}
}
