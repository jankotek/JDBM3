package jdbm.helper;

import java.io.ByteArrayOutputStream;

public class OpenByteArrayOutputStream extends ByteArrayOutputStream{
	
	
	public OpenByteArrayOutputStream(byte[] buf) {
		this.buf = buf;
	}


	public byte[] getBuf(){
		return buf;
	}

	
	public void reset(byte[] buf){
		this.buf = buf;
		this.count = 0;		
	}

}
