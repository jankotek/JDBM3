package jdbm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;

/**
 *
 */
public class Storage {

    /** maximal file size not rounded to block size */
    private final static long _FILESIZE = 1000000000l;
    /** maximal file size rounded to block size */
    private final long MAX_FILE_SIZE =  _FILESIZE - _FILESIZE % RecordFile.BLOCK_SIZE;


    private ArrayList<RandomAccessFile> rafs = new ArrayList<RandomAccessFile>();

    private String fileName;

    public Storage(String fileName) throws IOException {
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


    public void write(long offset, byte[] data) throws IOException {
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

    public void read(long offset, byte[] buffer, int nBytes) throws IOException {
        RandomAccessFile file = getRaf(offset);
        file.seek(offset%MAX_FILE_SIZE);
        int remaining = nBytes;
        int pos = 0;
        while (remaining > 0) {
            int read = file.read(buffer, pos, remaining);
            if (read == -1) {
                System.arraycopy(RecordFile.cleanData, 0, buffer, pos, remaining);
                break;
            }
            remaining -= read;
            pos += read;
        }
    }


    public String getFileName() {
        return fileName;
    }
}
