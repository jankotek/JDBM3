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

        Map<Integer,String> map = recman.createTreeMap("test");

        for(Integer i=1;i<1e8;i++){
            if(i%1e5==0) {
                System.out.println(i);
                recman.commit();
                //Thread.sleep(1000000);
            }
            map.put(i,"lalalala"+i.hashCode());
        }
        recman.commit();

        recman.close();

        System.out.println("Finished, total time: "+(System.currentTimeMillis()-startTime)/1000);
    }
}
