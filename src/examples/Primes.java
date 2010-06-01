import java.io.IOException;

import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;


/**
 * An example which searches Prime Numbers using  
 * <a href='http://en.wikipedia.org/wiki/Sieve_of_Eratosthenes'>Sieve of Eratosthenes</a> 
 * While the algorithm is very primitive, it well demonstrates JDBM usage.
 * 
 * @author Jan Kotek
 *
 */
public class Primes {
	
	static final long LIMIT = (long) 1e6;
	
	
	public static void main(String[] args) throws IOException {

		// let RecordManager to do commits automatically every 10000 inserts
		RecordManager recMan = RecordManagerFactory.createRecordManager("primes");
		PrimaryTreeMap<Long, Object> siege = recMan.treeMap("siege");
		//dummy used as value in map, since JDBM does not handle null values
		final Long dummyValue = new Long(0); 
		for(Long number=new Long(2);number<LIMIT;number++){
			//if siege does not contain it, it is prime
			if(!siege.containsKey(number)){
				//print number
				System.out.print("Found prime: "+number +", filling siege ");
				
				//put rest of numbers into siege
				for(Long si=number;si<LIMIT;si+=number){
					siege.put(si, dummyValue);
				}
				//save changes, if LIMIT will be too big, continuous commits will be needed
				recMan.commit();
				System.out.println("- DONE");
				
			}
		}
		recMan.close();
	}	
	
	
}
