package net.kotek.jdbm;

import java.io.File;
import java.io.IOException;
import java.util.Set;
//TODO Make this work!!
public class StorageZipTest extends TestCaseWithTestFile {



    public void test_archive_creation() throws IOException {

        File tmp = File.createTempFile("JDBM_TEST_ZIP", ".zip");
        String dbpath = tmp.getPath() ;
        tmp.deleteOnExit();

        //first create archie and put it in zip file
        DBStore r = new DBStore(newTestFile(), false, true);
        Set<Long> h = r.createHashSet("hash");
        for (Long l = 0L; l < 1e3; l++) {
            h.add(l);
        }
        r.commit();
        r.copyToZip(dbpath);
        r.close();

        System.out.println("Zip file created, size: " + tmp.length());

        //open zip file and check it contains all data
        DB r2 =  DBMaker.openZip(dbpath).readonly().make();

        Set<Long> h2 = r2.getHashSet("hash");
        for (Long l = 0L; l < 1e3; l++) {
            assertTrue(h2.contains(l));
        }

    }

}
