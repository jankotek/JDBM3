package snippet;

public class Snippet {
	public static void main(String[] args) {
		try{
		Object[] o = null;
		System.out.println("aa");
		while (true) { 
			
			o = new Object[] { o };
			
		}
		
		}catch (Throwable e){
			e.printStackTrace();
		}
	}
}

