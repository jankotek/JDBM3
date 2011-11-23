package jdbm;

import junit.framework.TestCase;
import jdbm.SerialClassInfoTest.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ObjectOutputStreamTest extends TestCase {


    <E> E neser(E e) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream i = new ByteArrayOutputStream();
        new ObjectOutputStream(i).writeObject(e);
        return (E) new ObjectInputStream(new ByteArrayInputStream(i.toByteArray())).readObject();
    }

    public void testSimple() throws ClassNotFoundException, IOException {

        Bean1 b = new Bean1("qwe","rty");
        Bean1 b2 = neser(b);

        assertEquals(b,b2);

    }
}
