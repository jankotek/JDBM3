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

import java.io.File;
import java.io.IOException;

import junit.framework.TestSuite;

/**
 *  This class contains all Unit tests for {@link RecordFile}.
 */
final public class RecordFileTest
    extends TestCaseWithTestFile
{


    public static void deleteFile( String filename )
    {
        File file = new File( filename );

        if ( file.exists() ) {
            try {
                file.delete();
            } catch ( Exception except ) {
                except.printStackTrace();
            }
            if ( file.exists() ) {
                System.out.println( "WARNING:  Cannot delete file: " + file );                
            }
        }
    }


    /**
     *  Test constructor
     */
    public void testCtor()
        throws Exception
    {
        RecordFile file = newRecordFile();
        file.close();
    }


    /**
     *  Test addition of record 0
     */
    public void testAddZero()
        throws Exception
    {
    	String f = newTestFile();
        RecordFile file = new RecordFile(f,false);
        byte[] data = file.get( 0 ).getData();
        data[ 14 ] = (byte) 'b';
        file.release( 0, true );
        file.close();
        file = new RecordFile(f,false);
        data = file.get( 0 ).getData();
        assertEquals( (byte) 'b', data[ 14 ] );
        file.release( 0, false );
        file.close();
    }


    /**
     *  Test addition of a number of records, with holes.
     */
    public void testWithHoles()
        throws Exception
    {
    	String f = newTestFile();
        RecordFile file = new RecordFile(f,false);

        // Write recid 0, byte 0 with 'b'
        byte[] data = file.get( 0 ).getData();
        data[ 0 ] = (byte) 'b';
        file.release( 0, true );

        // Write recid 10, byte 10 with 'c'
        data = file.get( 10 ).getData();
        data[ 10 ] = (byte) 'c';
        file.release( 10, true );

        // Write recid 5, byte 5 with 'e' but don't mark as dirty
        data = file.get( 5 ).getData();
        data[ 5 ] = (byte) 'e';
        file.release( 5, false );

        file.close();

        file = new RecordFile(f,false);
        data = file.get( 0 ).getData();
        assertEquals( "0 = b", (byte) 'b', data[ 0 ] );
        file.release( 0, false );

        data = file.get( 5 ).getData();
        assertEquals( "5 = 0", 0, data[ 5 ] );
        file.release( 5, false );

        data = file.get( 10 ).getData();
        assertEquals( "10 = c", (byte) 'c', data[ 10 ] );
        file.release( 10, false );

        file.close();
    }


    /**
     *  Test wrong release
     */
    public void testWrongRelease()
        throws Exception
    {
        RecordFile file = newRecordFile();

        // Write recid 0, byte 0 with 'b'
        byte[] data = file.get( 0 ).getData();
        data[ 0 ] = (byte) 'b';
        try {
            file.release( 1, true );
            fail( "expected exception" );
        } catch ( IOException except ) {
            // ignore
        }
        file.release( 0, false );

        file.close();

        // @alex retry to open the file
        /*
        file = new RecordFile( testFileName );
        PageManager pm = new PageManager( file );
        pm.close();
        file.close();
        */
    }


}
