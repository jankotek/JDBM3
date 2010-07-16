package jdbm.btree;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import jdbm.helper.LongPacker;



/**
 * Provider for implementations {@link ByteArrayCompressor}suitable for storing
 * binary data, {@link BinaryCompressionProvider}, or {@link String} data,
 * {@link StringCompressionProvider}.
 * 
 * @author Kevin Day
 */

class LeadingValueCompressionProvider {


	/**
	 * Read previously written data
	 */	
    static byte[] readByteArray( DataInputStream in, byte[] previous, int ignoreLeadingCount ) throws IOException
	{
	    int len = LongPacker.unpackInt(in) -1;
	    if (len == -1)
	    	return null;
	    
	    int actualCommon = 0;
	    
    	actualCommon = LongPacker.unpackInt(in);

	    
	    byte[] buf = new byte[ len ];

	    if (previous == null){
	    	actualCommon = 0;
	    }
	    
	    
    	if (actualCommon > 0){
    		in.readFully( buf, 0, ignoreLeadingCount);
    		System.arraycopy(previous, ignoreLeadingCount, buf, ignoreLeadingCount, actualCommon - ignoreLeadingCount);
    	}
	    in.readFully( buf, actualCommon, len - actualCommon );
	    return buf;
	}
	
    /**
     * Writes the contents of buf to the DataOutput out, with special encoding if
     * there are common leading bytes in the previous group stored by this compressor.
     */
	static void writeByteArray( DataOutputStream out, byte[] buf, byte[] previous, int ignoreLeadingCount ) throws IOException
	{
	    if ( buf == null ) {
	        LongPacker.packInt(out, 0);
	        return;
	    }
	    
    	int actualCommon = ignoreLeadingCount;

    	if (previous != null){
	    	int maxCommon = buf.length > previous.length ? previous.length : buf.length;
	   
	    	if (maxCommon > Short.MAX_VALUE) maxCommon = Short.MAX_VALUE;
	    	
	    	for (; actualCommon < maxCommon; actualCommon++) {
				if (buf[actualCommon] != previous[actualCommon])
					break;
			}
    	}
	     

       	// there are enough common bytes to justify compression
       	LongPacker.packInt(out,buf.length+1 );// store as +1, 0 indicates null
       	LongPacker.packInt(out,actualCommon );
       	out.write( buf, 0, ignoreLeadingCount);
       	out.write( buf, actualCommon, buf.length - actualCommon );
	    
	}	
	
    
 
    
//    static String findCommonStringPrefix(Object[] strs) {
//    	int commonChars = 1;
//
//    	String minimalString = null;
//    	int minimalSize =Integer.MAX_VALUE;
//    	//find first non null string and minimal size
//    	for(Object s2:strs){
//    		String s = (String) s2;
//    		if(s!=null && minimalSize>s.length()){
//    			minimalSize = s.length();
//    			minimalString = s;
//    		}
//    	}
//    	//all null
//    	if(minimalString == null)
//    		return null;
//    	
//    	String previousPrefix = "";
//    	while(commonChars<=minimalSize){
//    		String prefix = minimalString.substring(0,commonChars);
//    		//check if all strings stars with the same prefix
//    		for(Object s2:strs){
//    			String s = (String) s2;
//    			if(s!=null && !s.startsWith(prefix)){    				
//    				//does not start, return previous
//    				return previousPrefix;
//    			}
//    		}
//    		
//    		//all ok, add one more common byte
//    		previousPrefix = prefix;
//    		commonChars++; 
//    	}
//    	return previousPrefix;
//		
//	}

}
