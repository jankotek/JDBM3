import java.io.IOException;

import jdbm.*;

/**
 * This program demonstrates basic JDBM usage.
 * 
 * @author Jan Kotek
 *
 */
public class HelloWorld {
	public static void main(String[] args) throws IOException {

		/** create (or open existing) database */
		String fileName = "helloWorld";
		RecordManager recMan = new RecordManagerBuilder(fileName).build();
		
		/** Creates TreeMap which stores data in database.  
		 *  Constructor method takes recordName (something like SQL table name)*/
		String recordName = "firstTreeMap";
		PrimaryTreeMap<Integer,String> treeMap = recMan.treeMap(recordName); 

		/** add some stuff to map*/
		treeMap.put(1, "One");
		treeMap.put(2, "Two");
		treeMap.put(3, "Three");
		
		System.out.println(treeMap.keySet());
		// > [1, 2, 3]
		
		/** Map changes are not persisted yet, commit them (save to disk) */
		recMan.commit();

		System.out.println(treeMap.keySet());
		// > [1, 2, 3]

		/** Delete one record. Changes are not commited yet, but are visible. */
		treeMap.remove(2);

		System.out.println(treeMap.keySet());
		// > [1, 3]
		
		/** Did not like change. Roolback to last commit (undo record remove). */
		recMan.rollback();
		
		/** Key 2 was recovered */
		System.out.println(treeMap.keySet());
		// > [1, 2, 3]
		
		/** close record manager */
		recMan.close();

	}
}
