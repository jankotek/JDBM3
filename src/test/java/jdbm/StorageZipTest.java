package jdbm;

import javax.annotation.processing.Filer;
import java.io.File;
import java.io.IOException;
import java.util.Set;

public class StorageZipTest extends TestCaseWithTestFile {

    public void test_archive_creation() throws IOException {

        File tmp = File.createTempFile("JDBM_TEST_ZIP","zip");
        String dbpath = tmp.getPath()+"!/test/db";
        tmp.deleteOnExit();

        //first create archie and put it in zip file
        RecordManager r = newRecordManager();
        Set<Long> h = r.createHashSet("hash");
        for(Long l=0L;l<1e5;l++){
            h.add(l);
        }
        r.commit();
        r.copyToZipStore(dbpath);
        r.close();

        System.out.println("Zip file created, size: "+tmp.length());

        //open zip file and check it contains all data
        RecordManager r2 = new RecordManagerBuilder(dbpath).readonly().build();
        Set<Long> h2 = r2.loadHashSet("hash");
        for(Long l=0L;l<1e5;l++){
            assertTrue(h2.contains(l));
        }

    }

}
