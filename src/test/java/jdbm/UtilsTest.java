package jdbm;

import junit.framework.TestCase;

public class UtilsTest extends TestCase {

    public void testFormatSpaceUsage(){
        assertEquals("100B",Utils.formatSpaceUsage(100L));
        assertEquals("1024B",Utils.formatSpaceUsage(1024L));
        assertEquals("10KB",Utils.formatSpaceUsage(10024L));
        assertEquals("15MB",Utils.formatSpaceUsage(15000000));
    }
}
