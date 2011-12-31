package jdbm.junk;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * This demonstrates MappedByteBuffer behaviour when file size is expanding
 */
public class MappedBufferGrow {

    public static void main(String[] args) throws IOException {
        ///File f = File.createTempFile("aaa","aaa");
        File f = new File("test");
        f.deleteOnExit();

        RandomAccessFile raf = new RandomAccessFile(f, "rw");

        raf.setLength((long) 1e6);

        System.out.println("length is " + raf.length());


        raf.seek((long) 2e6);
        raf.write(1);

        System.out.println("length is " + raf.length());

        MappedByteBuffer b = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, (long) 3e6);

        System.out.println("length after mapping is " + raf.length());

        b.position((int) (3e6 - 10));
        b.put((byte) 1);

        b.force();

        System.out.println("length after writting to MappedByteBuffer is " + raf.length());


    }
}
