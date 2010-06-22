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
 * Comparator for byte arrays.
 *
 * @author <a href="mailto:boisvert@intalio.com">Alex Boisvert</a>
 * @version $Id: ByteArrayComparator.java,v 1.4 2002/05/31 06:33:20 boisvert Exp $
 */
public final class ByteArrayComparator
    implements Comparator<byte[]>, Serializable
{

    /**
     * Version id for serialization.
     */
    final static long serialVersionUID = 1L;


    /**
     * Compare two objects.
     *
     * @param obj1 First object
     * @param obj2 Second object
     * @return a positive integer if obj1 > obj2, 0 if obj1 == obj2,
     *         and a negative integer if obj1 < obj2
     */
     public int compare( byte[] obj1, byte[] obj2 )
     {
        if ( obj1 == null ) {
            throw new IllegalArgumentException( "Argument 'obj1' is null" );
        }

        if ( obj2 == null ) {
            throw new IllegalArgumentException( "Argument 'obj2' is null" );
        }

        return compareByteArray( obj1,  obj2 );
     }


    /**
     * Compare two byte arrays.
     */
    public static int compareByteArray( byte[] thisKey, byte[] otherKey )
    {
        int len = Math.min( thisKey.length, otherKey.length );

        // compare the byte arrays
        for ( int i=0; i<len; i++ ) {
            if ( thisKey[i] >= 0 ) {
                if ( otherKey[i] >= 0 ) {
                    // both positive
                    if ( thisKey[i] < otherKey[i] ) {
                        return -1;
                    } else if ( thisKey[i] > otherKey[i] ) {
                        return 1;
                    }
                } else {
                    // otherKey is negative => greater (because MSB is 1)
                    return -1;
                }
            } else {
                if ( otherKey[i] >= 0 ) {
                    // thisKey is negative => greater (because MSB is 1)
                    return 1;
                } else {
                    // both negative
                    if ( thisKey[i] < otherKey[i] ) {
                        return -1;
                    } else if ( thisKey[i] > otherKey[i] ) {
                        return 1;
                    }
                }
            }
        }
        if ( thisKey.length == otherKey.length) {
            return 0;
        }
        if ( thisKey.length < otherKey.length ) {
            return -1;
        }
        return 1;
    }

}
