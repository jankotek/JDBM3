
package jdbm.btree;

import java.io.IOException;
import java.util.Properties;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;

/**
 * Test BTree insert performance.
 */
public class TestInsertPerf 
{

    int _numberOfObjects;

    public TestInsertPerf( int numberOfObjects ) {
        _numberOfObjects = numberOfObjects;
    }

    public void insert()
        throws IOException
    {

        BTree    btree;
        RecordManager  recman;
        long           start, finish;
        Properties     props;
        
        props = new Properties();
        recman = RecordManagerFactory.createRecordManager( "TestInsertPref-" + System.currentTimeMillis(),
                                                           props);
        btree = BTree.createInstance( recman);
        
        // Note:  One can use specialized serializers for better performance / database size
        // btree = BTree.createInstance( recman, new LongComparator(),
        //                               LongSerializer.INSTANCE, IntegerSerializer.INSTANCE );

        start = System.currentTimeMillis();
        for (int i = 0; i < _numberOfObjects; i++) {
            btree.insert( new Long( i ), new Integer( i ), false );
        }
        recman.commit();
        finish = System.currentTimeMillis();
        
        System.out.println( "It took " + (finish - start) + " ms to insert "
                            + _numberOfObjects +" objects." );
                                
    }
    
    
    public static void main( String[] args ) {
        if ( args.length != 1 ) {
            System.out.println( "Usage:  TestInsertPerf [numberOfObjects]" );
            return;
        }
        int numberOfObjects = Integer.parseInt( args[ 0 ] );
        TestInsertPerf test = new TestInsertPerf( numberOfObjects );
        try {
            test.insert();
        } catch ( IOException except ) {
            except.printStackTrace();   
        }
    }

}
