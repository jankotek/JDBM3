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

import java.io.*;
import java.util.Map;

/**
 *  This class contains all Unit tests for {@link HTreeBucket}.
 *
 *  @author Alex Boisvert
 */
public class HTreeBucketTest extends TestCaseWithTestFile {




    /**
     *  Basic tests
     */
    public void testBasics() throws IOException {

        RecordManager recman = newRecordManager();

        HTree tree = (HTree) recman.createHashMap("test");

        HTreeBucket bucket = new HTreeBucket(tree, 0);

        // add
        bucket.addElement("key", "value");
        String s = (String)bucket.getValue("key");
        assertEquals("value", s);

        // replace
        bucket.addElement("key", "value2");
        s = (String)bucket.getValue("key");
        assertEquals("value2", s);

        // add
        bucket.addElement("key2", "value3");
        s = (String)bucket.getValue("key2");
        assertEquals("value3", s);

        // remove
        bucket.removeElement("key2");
        s = (String)bucket.getValue("key2");
        assertEquals(null, s);
        bucket.removeElement("key");
        s = (String)bucket.getValue("key");
        assertEquals(null, s);

        recman.close();
    }

    public static class LongSerializer implements Serializer<Long> {

            public LongSerializer() {

            }

            public void serialize(DataOutput out, Long obj) throws IOException {
                out.writeLong(obj);
            }

            public Long deserialize(DataInput in) throws IOException, ClassNotFoundException {
                return in.readLong();
            }
        }

    public void testCustomSerializer() throws IOException {
        Serializer<Long> ser = new LongSerializer();


        RecordManager recman = newRecordManager();
        Map<Long,Long> s = recman.createHashMap("test", ser, ser);

        s.put(new Long(1),new Long(2));
        s.put(new Long(4), new Long(5));
        recman.commit();
        recman.clearCache();
        assertTrue(s.size()==2);
        assertEquals(s.get(new Long(1)),new Long(2));
        assertEquals(s.get(new Long(4)),new Long(5));


    }
}
