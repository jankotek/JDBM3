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


package jdbm;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;

/**
 *  Random insertion/removal test for B+Tree data structure.
 *
 *  @author <a href="mailto:boisvert@exoffice.com">Alex Boisvert</a>
 */
public class BTreeBench extends TestCaseWithTestFile {


	RecordManager recman;

    /**
     * Test w/o compression or specialized key or value serializers.
     * 
     * @throws IOException
     */
    public void test_001() throws IOException {
    	recman = newRecordManager();
        BTree<Long,Long> tree = BTree.createInstance(recman);
        doTest( recman, tree, 5001 );
        recman.close();
    }
    
    
    public static void doTest( RecordManager recman, BTree<Long,Long> tree, int ITERATIONS )
    	throws IOException
    {

        long beginTime = System.currentTimeMillis();
        Hashtable<Long,Long> hash = new Hashtable<Long,Long>();

        for ( int i=0; i<ITERATIONS; i++) {
            Long random = new Long( random( 0, 64000 ) );

            if ( ( i % 5000 ) == 0 ) {
                long elapsed = System.currentTimeMillis() - beginTime;
                System.out.println( "Iterations=" + i + " Objects=" + tree.size()+", elapsed="+elapsed+"ms" );
                recman.commit();
            }
            if ( hash.get( random ) == null ) {
                //System.out.println( "Insert " + random );
                hash.put( random, random );
                tree.insert( random, random, false );
            } else {
                //System.out.println( "Remove " + random );
                hash.remove( random );
                Object removed = (Object) tree.remove( random );
                if ( ( removed == null ) || ( ! removed.equals( random ) ) ) {
                    throw new IllegalStateException( "Remove expected " + random + " got " + removed );
                }
            }
            // tree.assertOrdering();
            compare( tree, hash );
        }

    }
    
    static long random( int min, int max ) {
        return Math.round( Math.random() * ( max-min) ) + min;
    }

    static void compare( BTree<Long,Long> tree, Hashtable<Long,Long> hash ) throws IOException {
        boolean failed = false;
        Enumeration<Long> enumeration;

        if ( tree.size() != hash.size() ) {
            throw new IllegalStateException( "Tree size " + tree.size() + " Hash size " + hash.size() );
        }

        enumeration = hash.keys();
        while ( enumeration.hasMoreElements() ) {
            Long key = enumeration.nextElement();
            Long hashValue = hash.get( key );
            Long treeValue = tree.get(key);
            if ( ! hashValue.equals( treeValue ) ) {
                System.out.println( "Compare expected " + hashValue + " got " + treeValue );
                failed = true;
            }
        }
        if ( failed ) {
            throw new IllegalStateException( "Compare failed" );
        }
    }

}
