package net.kotek.jdbm.junk;

import net.kotek.jdbm.DB;
import net.kotek.jdbm.DBMaker;

import java.io.IOException;
import java.util.Map;

/**
 * Creates huge file
 */
public class HugeData {

    static public void main(String[] args) throws IOException, InterruptedException {

        long startTime = System.currentTimeMillis();
        //new File("/media/b0beb325-d9fe-4a30-9f58-77e6b15e6b7d/lost+found/large/").mkdirs();
        DB db = DBMaker.openFile("/media/b0beb325-d9fe-4a30-9f58-77e6b15e6b7d/db")
                .disableTransactions()
                .make();

        Map<Long,Integer> map = db.createTreeMap("test");
//        List<Long> test = db.createLinkedList("test");
        final double max = 1e10;

        for (Long i = 1L; i < max; i++) {
            if (i % 1e6 == 0) {
                System.out.println(i + " - " +(100D * i /max) + " %");
                //Thread.sleep(1000000);
            }
//            test.add(i);
            map.put(i,i.hashCode());
        }

        db.defrag(true);
        db.close();

        System.out.println("Finished, total time: " + (System.currentTimeMillis() - startTime) / 1000);
    }
}
