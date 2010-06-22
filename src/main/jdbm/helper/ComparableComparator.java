/*******************************************************************************
 * Copyright 2010 Cees De Groot, Alex Boisvert, Jan Kotek
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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
