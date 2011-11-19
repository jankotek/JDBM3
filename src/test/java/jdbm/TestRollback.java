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

/**
 *  Test cases for HTree rollback
 */
public class TestRollback
    extends TestCaseWithTestFile
{


    /**
     * Test case courtesy of Derek Dick (mailto:ddick  users.sourceforge.net)
     */
    public void testRollback1() 
        throws Exception
    {

        // Note: We start out with an empty file
        RecordManageAbstract recman =  newRecordManager();

        long root = recman.getNamedObject( "xyz" );
			 			
        HTree<String,String> tree = null;
        if ( root == 0 ) {
            // create a new one
            tree = new HTree( recman );
            root = tree.getRecid();
            recman.setNamedObject( "xyz", root );
            recman.commit();
        } else {
            tree = new HTree( recman, root );
        }

        tree.put( "Foo", "Bar" );
        tree.put( "Fo", "Fum" );

        recman.commit();

        tree.put( "Hello", "World" );

        recman.rollback();

        tree = new HTree( recman, root );
        assertTrue( tree.find( "Foo" ).equals( "Bar" ) );
        assertTrue( tree.find( "Fo" ).equals( "Fum" ) );
        assertTrue( tree.find( "Hello" ) == null );
    }		

    
    /**
     * Test case courtesy of Derek Dick (mailto:ddick  users.sourceforge.net)
     */
    public void testRollback2() 
        throws Exception
    {
        RecordManageAbstract recman;
        long root;

        // Note: We start out with an empty file
        recman = newRecordManager();

        root = recman.getNamedObject( "xyz" );

        HTree tree = null;
        if ( root == 0 ) {
            // create a new one
            tree = new HTree( recman );
            root = tree.getRecid();
            recman.setNamedObject( "xyz", root );
            recman.commit();
        } else {
            tree = new HTree( recman, root );
        }

        tree.put( "hello", "world" );
        tree.put( "goodnight", "gracie" );
        recman.commit();

        tree.put( "derek", "dick" );
        recman.rollback();

        assertTrue( tree.find( "derek" ) == null );
        assertTrue( tree.find( "goodnight" ).equals( "gracie" ) );
        assertTrue( tree.find( "hello" ).equals( "world" ) );
    }
	    
    
    /**
     * Test case courtesy of Derek Dick (mailto:ddick  users.sourceforge.net)
     */
    public void testRollback1b() 
        throws Exception
    {

        // Note: We start out with an empty file
        RecordManageAbstract recman =  newRecordManager();

        long root = recman.getNamedObject( "xyz" );
			 			
        BTree<String,String> tree = null;
        if ( root == 0 ) {
            // create a new one
            tree = BTree.createInstance( recman );
            root = tree.getRecid();
            recman.setNamedObject( "xyz", root );
            recman.commit();
        } else {
            tree = BTree.load( recman, root );
        }

        tree.insert( "Foo", "Bar",true );
        tree.insert( "Fo", "Fum",true );

        recman.commit();

        tree.insert( "Hello", "World",true );

        recman.rollback();

        tree = BTree.load( recman, root );
        assertTrue( tree.get("Foo").equals( "Bar" ) );
        assertTrue( tree.get("Fo").equals( "Fum" ) );
        assertTrue( tree.get("Hello") == null );
    }		

    
    /**
     * Test case courtesy of Derek Dick (mailto:ddick  users.sourceforge.net)
     */
    public void testRollback2b() 
        throws Exception
    {
        RecordManageAbstract recman;
        long root;

        // Note: We start out with an empty file
        recman = newRecordManager();

        root = recman.getNamedObject( "xyz" );

        BTree tree = null;
        if ( root == 0 ) {
            // create a new one
            tree = BTree.createInstance( recman );
            root = tree.getRecid();
            recman.setNamedObject( "xyz", root );
            recman.commit();
        } else {
            tree = BTree.load( recman, root );
        }

        tree.insert( "hello", "world",true );
        tree.insert( "goodnight", "gracie",true );
        recman.commit();

        tree.insert( "derek", "dick",true );
        recman.rollback();

        assertTrue( tree.get("derek") == null );
        assertTrue( tree.get("goodnight").equals( "gracie" ) );
        assertTrue( tree.get("hello").equals( "world" ) );
    }
	    
    
    
}
