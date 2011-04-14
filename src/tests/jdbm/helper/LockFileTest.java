package jdbm.helper;

import jdbm.recman.TestCaseWithTestFile;

import java.io.File;
import java.io.IOException;

public class LockFileTest extends TestCaseWithTestFile {

    public void testLock() throws IOException {
        File f = new File(newTestFile());

        LockFile l1 = new LockFile(f);
        l1.lock();

        LockFile l2 = new LockFile(f);
        try{
            l2.lock();
            fail("should throw an exception");
        }catch(Exception e){
            //expected
        }

        l1.unlock();
        l2.lock();
        l2.unlock();

    }

}
