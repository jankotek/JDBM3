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
import java.io.PrintStream;
import java.util.Iterator;

/**
 * Test case provided by Daniel Herlemont to demonstrate a bug in
 * HashDirectory.  The returned Enumeration got into an infinite loop
 * on the same key/val pair.
 *
 */
public class HashtableTest {

    private RecordManager recman;
    private HTree<String, String> hashtable;

    private boolean enumerate = false;
    private boolean populate = false;
    private boolean retrieve = false;
    private String jdbmName = "hashtest";
    private String name = "hashtable";
    private String onekey = "onekey";


    /**
     * Initialize RecordManager and HTree
     */
    protected void init()
        throws IOException
    {
        recman = new RecordManagerBuilder( jdbmName ).build();

        // create or reload HTree
        long recid = recman.getNamedObject( name );
        if ( recid == 0 ) {
            hashtable = new HTree( recman );
            recman.setNamedObject( name, hashtable.getRecid() );
        } else {
            hashtable = new HTree( recman, recid );
        }

    }


    /**
     * Populate HTree with some data
     */
    protected void populate()
        throws IOException
    {
        try {
            int max = 1000;
            for ( int i=0; i<max; i++ ) {
                String key = "key" + i;
                String val = "val" + i;
                hashtable.put( key,val );
                System.out.println( "put key=" + key + " val=" + val );
            }

            System.out.println( "populate completed" );
        } finally {
            recman.close();
        }
    }


    /**
     * Retrieve a given object based on key
     */
    protected Object retrieve( String key )
        throws IOException
    {
        init();

        try {
            String val = hashtable.find( key );
            System.out.println( "retrieve key=" + key + " val=" + val );
            return val;
        } finally {
            recman.close();
        }
    }


    /**
     * Enumerate keys and objects found in HTree
     */
    protected void enumerate()
        throws IOException
    {
        init();

        try {
            Iterator<String> iter = hashtable.keys();
            
            while (iter.hasNext() ) {
            	String key = iter.next();
                String val = hashtable.find( key );
                System.out.println( "enum key=" + key + " val=" + val );
            }
        } finally {
            recman.close();
        }
    }


    /**
     * Execute commands specified on command-line
     */
    protected void doCommands()
        throws IOException
    {
        if ( enumerate ) {
            enumerate();
        }

        if ( populate ) {
            populate();
        }

        if ( retrieve ) {
            retrieve( onekey );
        }
    }


    /**
     * Parse command-line arguments
     */
    protected void parseArgs( String args[] )
    {
        for ( int argn = 0; argn < args.length; argn++ ) {
            if ( args[ argn ].equals( "-enum" ) ) {
                enumerate = true;
            } else if ( args[ argn ].equals( "-populate" ) ) {
                populate = true;
            } else if ( args[ argn ].equals( "-retrieve" ) ) {
                retrieve = true;
            } else if ( args[ argn ].equals( "-jdbmName" ) && argn < args.length - 1 ) {
                jdbmName = args[ ++argn ];
            } else if (args[ argn ].equals( "-key" ) && argn < args.length - 1 ) {
                onekey = args[ ++argn ];
            } else if ( args[ argn ].equals( "-name" ) && argn < args.length - 1) {
                name = args[ ++argn ];
            } else {
                System.err.println( "Unrecognized option: " + args[ argn ] );
                usage( System.err );
            }
        }
    }


    /**
     * Display usage information
     */
    protected void usage( PrintStream ps )
    {
        ps.println( "Usage: java " + getClass().getName() + " Options" );
        ps.println();
        ps.println( "Options (with default values):" );
        ps.println( "-help print this" );
    }


    /**
     * Static program entrypoint
     */
    public static void main( String[] args )
    {
        HashtableTest instance = new HashtableTest();
        instance.parseArgs( args );
        try {
            instance.doCommands();
        } catch ( IOException except ) {
            except.printStackTrace();
        }
    }

}

