package jdbm;

import java.io.IOException;
import java.util.Map;

/**
 * Creates huge file
 */
public class HugeData {

    static public void main(String[] args) throws IOException, InterruptedException {

        long startTime = System.currentTimeMillis();
        RecordManager recman = new RecordManagerBuilder("/tmp/large/db"+Math.random())
                .disableTransactions().enableMRUCache()
                //.disableDiskSync()
                .build();

        Map<Long,Integer> map = recman.createTreeMap("test");

        for(Long i=1L;i<1e10;i++){
            if(i%1e6==0) {
                System.out.println(i);
                recman.commit();
                //Thread.sleep(1000000);
            }
            map.put(i,i.hashCode());
        }
        recman.commit();

        recman.close();

        System.out.println("Finished, total time: "+(System.currentTimeMillis()-startTime)/1000);
    }
}
