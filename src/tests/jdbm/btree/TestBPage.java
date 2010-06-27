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


package jdbm.btree;

import java.io.IOException;

import jdbm.RecordManager;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;
import jdbm.recman.TestCaseWithTestFile;
import junit.framework.TestSuite;

/**
 *  This class contains all Unit tests for {@link Bpage}.
 *
 *  @author <a href="mailto:boisvert@exoffice.com">Alex Boisvert</a>
 *  @version $Id: TestBPage.java,v 1.5 2003/09/21 15:49:02 boisvert Exp $
 */
public class TestBPage extends TestCaseWithTestFile {



    /**
     *  Basic tests
     */
    public void testBasics() throws IOException {
        RecordManager recman;
        String test, test1, test2, test3;

        test = "test";
        test1 = "test1";
        test2 = "test2";
        test3 = "test3";

        recman = newRecordManager();



        BTree tree = BTree.createInstance( recman);

        BPage page = new BPage( tree, test, test );

        TupleBrowser browser;
        Tuple tuple = new Tuple();

        // test insertion
        page.insert( 1, test2, test2, false );
        page.insert( 1, test3, test3, false );
        page.insert( 1, test1, test1, false );

        // test binary search
        browser = page.find( 1, test2 );
        if ( browser.getNext( tuple ) == false ) {
            throw new IllegalStateException( "Browser didn't have 'test2'" );
        }
        if ( ! tuple.getKey().equals( test2 ) ) {
            throw new IllegalStateException( "Tuple key is not 'test2'" );
        }
        if ( ! tuple.getValue().equals( test2 ) ) {
            throw new IllegalStateException( "Tuple value is not 'test2'" );
        }

        recman.close();
        recman = null;
    }



}
