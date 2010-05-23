package jdbm.helper;

import java.io.Serializable;
import java.util.Comparator;

/**
 * 
 * Compares comparables. Default comparator for most of java types
 * 
 * @author Jan Kotek
 *
 * @param <A>
 */
@SuppressWarnings("unchecked") 
public class ComparableComparator<A extends Comparable>
	implements Comparator<A>,Serializable {	
	
	/** use this instance, dont allocate new*/
	public final static Comparator INSTANCE =  new ComparableComparator();

	private static final long serialVersionUID = 1678377822276476166L;
	
	/** everyone should use INSTANCE*/
	private ComparableComparator(){};

	public int compare(Comparable o1, Comparable o2) {
        return o1.compareTo(o2);
    }
}