package jdbm.helper;

import java.io.ByteArrayInputStream;

public class OpenByteArrayInputStream extends ByteArrayInputStream{

	public OpenByteArrayInputStream(byte[] buf) {
		super(buf);
	}
	
	public byte[] getBuf(){
		return buf;
	}
	
	
	public void reset(byte[] buf, int count){
		this.buf = buf;
		this.count = count;		
		this.pos = 0;
		this.mark = 0;
	}	

}
