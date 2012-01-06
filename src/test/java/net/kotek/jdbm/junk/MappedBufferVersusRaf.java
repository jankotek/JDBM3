package net.kotek.jdbm.junk;


import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

/**
 * This script compares performance of memory mapped buffer versus RandomAccessFile
 */
public class MappedBufferVersusRaf {

    public static final int NUNBER_OF_READS = (int) 1e7;
    public static final int FILE_SIZE = (int) 1e6;
    public static final int BUFFER_SIZE = 2048;

    public static void main(String[] args) throws IOException {

        File f = File.createTempFile("mapped", "mapped");
        f.deleteOnExit();

        byte[] buffer = new byte[BUFFER_SIZE];

        OutputStream o = new BufferedOutputStream(new FileOutputStream(f));

        for (int i = 0; i < FILE_SIZE; i += BUFFER_SIZE) {
            o.write(buffer);
        }
        o.close();

        System.out.println("File filled");


        //open as RAF and read file randomly
        long t = System.currentTimeMillis();
        RandomAccessFile raf = new RandomAccessFile(f, "r");
        Random r = new Random(0);
        ByteBuffer byteBuf = ByteBuffer.wrap(buffer);
        for (int i = 0; i < NUNBER_OF_READS; i++) {
            long pos = r.nextInt(FILE_SIZE - BUFFER_SIZE);
            raf.seek(pos);
            raf.readFully(buffer);

            //read some random numbers just as JDBM does
            byteBuf.getLong(10);
            byteBuf.getLong(100);
            byteBuf.getLong(500);
        }
        System.out.println("RAF took " + (System.currentTimeMillis() - t));

        //previous test was not so good, so try to map entire file into memory
        t = System.currentTimeMillis();
        FileChannel channel = raf.getChannel();
        r = new Random(0);
        MappedByteBuffer byteBuf3 = channel.map(FileChannel.MapMode.READ_ONLY, 0, raf.length());

        byteBuf3.load();


        for (int i = 0; i < NUNBER_OF_READS; i++) {
            int pos = r.nextInt(FILE_SIZE - BUFFER_SIZE);

            //read some random numbers just as JDBM does
            byteBuf3.getLong(pos + 10);
            byteBuf3.getLong(pos + 100);
            byteBuf3.getLong(pos + 500);
        }
        System.out.println("MappedByteBuffer took " + (System.currentTimeMillis() - t));


    }

}
