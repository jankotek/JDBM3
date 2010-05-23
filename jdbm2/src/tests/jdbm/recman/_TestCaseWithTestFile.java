package jdbm.recman;

import java.io.File;
import java.io.IOException;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import junit.framework.TestCase;

/**
 * Subclass from this class if you have any test cases that need to do file I/O. The
 * setUp() and tearDown() methods here will take care of cleanup on disk. 
 * 
 * @author cdegroot <cg@cdegroot.com>
 *
 */
public abstract class _TestCaseWithTestFile extends TestCase {

	public static final String testFolder = "_testdb";
//	public static final String testFileName = "test";

	public static void deleteFile(String filename) {
	    File file = new File( filename );
	
	    if ( file.exists() ) {
	        try {
	            file.delete();                
	        } catch ( Exception except ) {
	            except.printStackTrace();
	        }
	        if ( file.exists() ) {
	            
	            /*
	             * Since the same test file is reused over and over a failure to
	             * remove the old file can cause spurious failures for the tests
	             * that follow. In general this can be traced back to a test
	             * that failed to close the file so that Java is not
	             * willing/able to delete it in the setUp() for the next test.
	             * 
	             * Throwing an exception here generally means that you will
	             * catch the test whose tearDown() left the store open, since
	             * most test do (all should) delete the store file in their
	             * tearDown().
	             * 
	             * @todo We need a means to both report the exception observed
	             * by junit while still being able to tearDown the test.
	             * Unfortunately junit does not support this option for us. We
	             * basically need chained exceptions (multiple exceptions
	             * thrown, not just the initCause). This should be raised as an
	             * issue against junit.
	             */ 
	            
	            throw new RuntimeException( "WARNING:  Cannot delete file: " + file );
	            //System.out.println( "WARNING:  Cannot delete file: " + file );
	        }
	    }
	}
	

	public void setUp() throws Exception {
		File f = new File(testFolder);
		if(!f.exists())
			f.mkdirs();
	}

	public void tearDown() throws Exception {
		File f = new File(testFolder);
		if(f.exists()){
			for(File f2 : f.listFiles()){
				f2.deleteOnExit();
				f2.delete();			
			}
		}
	}
	
	static public String newTestFile(){
		return testFolder+ File.separator + "test"+System.nanoTime();
	}

	static public RecordFile newRecordFile() throws IOException{
		return new RecordFile(newTestFile());
	}

	static public RecordManager newRecordManager() throws IOException{
		return RecordManagerFactory.createRecordManager(newTestFile());
	}

}