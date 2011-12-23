package jdbm.junk;

import jdbm.DB;
import jdbm.DBMaker;

import java.io.IOException;
import java.util.List;

/**
 * Creates huge file
 */
public class HugeData {

    static public void main(String[] args) throws IOException, InterruptedException {

        long startTime = System.currentTimeMillis();
        DB db = new DBMaker("/tmp/large/db"+Math.random())
                .disableTransactions()
                .enableMRUCache()
                .build();

//        Map<Long,Integer> map = db.createHashMap("test");
        List<Long> test = db.createLinkedList("test");

        for(Long i=1L;i<1e10;i++){
            if(i%1e6==0) {
                System.out.println(i);
                //Thread.sleep(1000000);
            }
            test.add(i);
//            map.put(i,i.hashCode());
        }

        db.close();

        System.out.println("Finished, total time: "+(System.currentTimeMillis()-startTime)/1000);
    }
}
