import java.io.IOException;
import java.io.Serializable;

import jdbm.*;


/**
 * Demonstrates more advanced usage of JDBM:
 * Secondary maps, 1:N relations.
 * 
 * @author Jan Kotek
 *
 */
public class Persons1 {

	static class Person implements Serializable{
		/** field used for person identification (primary key)**/
		String name;
		/** persisted with Person (embedded field in JPA terms) **/
		Address adress;
		/** N:1 relation */
		String fatherName;
		
		/** constructor, getters and setters are excluded for simplicity */
		public Person(String name, Address adress,String fatherName) {
			super();
			this.name = name;
			this.adress = adress;
			this.fatherName = fatherName;
		}

		public String toString(){
			return "Person["+name+"]";
		}
		
		public int hashCode() {
			return name == null? 0 : name.hashCode();
		}


		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null || !(obj instanceof Person))
				return false;
			Person other = (Person) obj;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			return true;
		}
	
		
		
	}
	
	static class Address implements Serializable{
		String streetName;
		String town;
		String country;
		
		public Address(String streetName, String town, String country) {
			super();
			this.streetName = streetName;
			this.town = town;
			this.country = country;
		}
		
		
	}
		
	public static void main(String[] args) throws IOException {
		//init Record Manager and dao
		RecordManager recman = new RecordManagerBuilder("persons1").build();

		PrimaryTreeMap<String,Person> personsByName = recman.treeMap("personsByName");

		SecondaryTreeMap<String, String, Person> personsByTown = 
				personsByName.secondaryTreeMap("personsByTown", 
						new SecondaryKeyExtractor<String, String, Person>() {
							public String extractSecondaryKey(String key,Person value) {
								return value.adress.town;
							}
						});

		
		//create a few persons
		Person patrick = new Person("Patrick Moore", 
				new Address("First street", "Athlone","Ireland"),
				null);
		personsByName.put(patrick.name, patrick);

		Person jack = new Person("Jack Moore", 
				new Address("First street", "Athlone","Ireland"),
				patrick.name);	
		personsByName.put(jack.name, jack);

		Person paul = new Person("Paul Moore", 
				new Address("Shop street", "Galway","Ireland"),
				patrick.name);
		personsByName.put(paul.name, paul	);

		
					
		System.out.println("Number of persons: "+personsByName.size());
		
		System.out.println("Persons with name Patrick Moore: "+personsByName.get("Patrick Moore"));
		System.out.println("Name of persons living in Galway: "+personsByTown.get("Galway"));
		System.out.println("Father of Paul Moore: "+
				personsByName.get(
						personsByName.get("Paul Moore").fatherName
					));

	}
	
}
