import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import jdbm.InverseHashView;
import jdbm.PrimaryStoreMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.SecondaryKeyExtractor;
import jdbm.SecondaryTreeMap;
import jdbm.Serializer;
import jdbm.helper.Serialization;


/**
 * Demonstrates more advanced usage of JDBM:
 * Secondary maps, 1:N relations.
 * 
 * @author Jan Kotek
 *
 */
public class Persons2 {

	static class Person implements Serializable{
		/** field used for person identification (primary key)**/
		String name;
		/** persisted with Person (embedded field in JPA terms) **/
		Address adress;
		/** N:1 relation */
		Person father;
		
		/** constructor, getters and setters are excluded for simplicity */
		public Person(String name, Address adress,Person father) {
			super();
			this.name = name;
			this.adress = adress;
			this.father = father;
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
	
	/** dao object which handles Person persistence */
	static class PersonDao implements Serializer<Person>{
		
		/** 
		 * This is map in which persons are inserted in. 
		 * Key is number assigned by store (recordId). 
		 * You should prefer recordId as primary index, it is more flexible.   
		 */
		PrimaryStoreMap<Long,Person> persons;
		
		/** Inverse view on person, it helps to find recordId which belongs to person */
		InverseHashView<	Long, Person> personsInverse;
		
		/** 
		 * Secondary view on persons, which identifies person by name. 
		 * This map is readonly, it is autoupdated by JDBM as primary map changes. 
		 * Key is name, value is recordId ( key from primary map), 
		 * third parameter is Person (value from primary map)
		 */
		SecondaryTreeMap<String, Long, Person> personsByName;

		/** 
		 * Secondary view on persons, which identifies person its town 
		 * This map is readonly, it is autoupdated by JDBM as primary map changes. 
		 * Key is town name extracted from primary table, 
		 * value is recordId ( key from primary map), 
		 * third parameter is town from  (value from primary map) 
		 */
		SecondaryTreeMap<String, Long, Person> personsByTown;
		
		
		public PersonDao(RecordManager recman) {
			persons = recman.storeMap("persons",this);
			personsInverse = persons.inverseHashView("personsInverse");
			personsByName = persons.secondaryTreeMap("personsByName",
						new SecondaryKeyExtractor<String, Long, Person>() {
							public String extractSecondaryKey(Long key, Person value) {
								return value.name;
							}});
			personsByTown = persons.secondaryTreeMap("personsByTown",
						new SecondaryKeyExtractor<String, Long, Person>() {
							public String extractSecondaryKey(Long key, Person value) {
								if(value.adress == null)
									return null;
								return value.adress.town;
							}});
			
		}
		
		public Person personByRecordId(Long recid){
			return persons.get(recid);
		}

		public Person personByName(String name){
			if(!personsByName.containsKey(name))
				return null;
			Iterator<Long> iter = personsByName.get(name).iterator();
			if(iter.hasNext())
				return personsByName.getPrimaryValue(iter.next());
			else 
				return null;
		}
		
		public Iterable<Person> personsByTown(String name){
			List<Person> ret = new ArrayList<Person>(); 
			for(Long l: personsByTown.get(name)){
				ret.add(personsByTown.getPrimaryValue(l));
			}
			return ret;
		}

		
		public void insertPerson(Person person){
			Long recid = personsInverse.findKeyForValue(person);
			if(recid == null)
				persons.putValue(person);
			else
				persons.put(recid, person);
		}

		public Person deserialize(DataInputStream in) throws IOException, ClassNotFoundException {
			String name = (String) Serialization.readObject(in);
			Address address = (Address) Serialization.readObject(in);
			Person father = persons.get((Long) Serialization.readObject(in));
			Person p = new Person(name,address,father);			
			
			return p;
		}

		public void serialize(DataOutputStream out, Person obj)
				throws IOException {
			Serialization.writeObject(out, obj.name);
			Serialization.writeObject(out, obj.adress);
			Serialization.writeObject(out, findOrPersistPerson(obj.father));
		}
		
		protected Long findOrPersistPerson(Person person){
			if(person == null)
				return null;
			Long recid = personsInverse.findKeyForValue(person);
			if(recid == null)
				recid = persons.putValue(person);
			return recid;
		}

	}
	
	public static void main(String[] args) throws IOException {
		//init Record Manager and dao
		RecordManager recman = RecordManagerFactory.createRecordManager("persons2");
		PersonDao dao = new PersonDao(recman);
		
		//create a few persons
		Person patrick = new Person("Patrick Moore", 
				new Address("First street", "Athlone","Ireland"),
				null);

		Person jack = new Person("Jack Moore", 
				new Address("First street", "Athlone","Ireland"),
				patrick);
		

		Person paul = new Person("Paul Moore", 
				new Address("Shop street", "Galway","Ireland"),
				patrick);

		
		
		//now store all this stuff		
		dao.insertPerson(jack);
		dao.insertPerson(patrick);
		dao.insertPerson(paul);
				
		System.out.println("Number of persons: "+dao.persons.size());
		
		System.out.println("Persons with name Patrick Moore: "+dao.personByName("Patrick Moore"));
		System.out.println("Persons living in Galway: "+dao.personsByTown("Galway"));
		System.out.println("Father of Paul Moore: "+dao.personByName("Paul Moore").father);

	}
	
}
