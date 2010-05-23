/*
 *  $Id: TestRecordFile.java,v 1.4 2001/11/10 18:36:50 boisvert Exp $
 *
 *  Unit tests for RecordFile class
 *
 *  Simple db toolkit
 *  Copyright (C) 1999, 2000 Cees de Groot <cg@cdegroot.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Library General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Library General Public License for more details.
 *
 *  You should have received a copy of the GNU Library General Public License
 *  along with this library; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 */
package jdbm.recman;

import java.io.File;
import java.io.IOException;

import junit.framework.TestSuite;

/**
 *  This class contains all Unit tests for {@link RecordFile}.
 */
final public class TestRecordFile
    extends _TestCaseWithTestFile
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
        RecordFile file = new RecordFile(f);
        byte[] data = file.get( 0 ).getData();
        data[ 14 ] = (byte) 'b';
        file.release( 0, true );
        file.close();
        file = new RecordFile(f);
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
        RecordFile file = new RecordFile(f);

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

        file = new RecordFile(f);
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


    /**
     *  Runs all tests in this class
     */
    public static void main( String[] args )
    {
        junit.textui.TestRunner.run( new TestSuite( TestRecordFile.class ) );
    }
}
