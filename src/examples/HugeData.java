import java.io.IOException;

import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;


/** 
 * 
 * This examples generates huge map of data. 
 * It inserts 10 000 000 records, it takes about 10 minutes to finish. 
 * 
 * @author Jan Kotek
 *
 */
public class HugeData {
	public static void main(String[] args) throws IOException {

		/** open db */
        RecordManager recman = RecordManagerFactory.createRecordManager( "hugedata");        
        PrimaryTreeMap<Long, String> m = recman.treeMap("hugemap");
        
        /** insert 1e7 records */
        for(long i = 0;i<1e8;i++){
        	m.put(i, "aa"+i);        
        	if(i%1e5==0){
        		/** Commit periodically, otherwise program would run out of memory */         		 
        		recman.commit();
        		System.out.println(i);        		
        	}
        		
        }
        
        recman.commit();
        recman.close();
        System.out.println("DONE");
        
	}
}
