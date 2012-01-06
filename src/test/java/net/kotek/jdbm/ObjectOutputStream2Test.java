package net.kotek.jdbm;

import junit.framework.TestCase;
import net.kotek.jdbm.SerialClassInfoTest.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ObjectOutputStream2Test extends TestCase {


    <E> E neser(E e) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream i = new ByteArrayOutputStream();
        new ObjectOutputStream2(i).writeObject(e);
        return (E) new ObjectInputStream2(new ByteArrayInputStream(i.toByteArray())).readObject();
    }

    public void testSimple() throws ClassNotFoundException, IOException {

        Bean1 b = new Bean1("qwe", "rty");
        Bean1 b2 = neser(b);

        assertEquals(b, b2);

    }
}
