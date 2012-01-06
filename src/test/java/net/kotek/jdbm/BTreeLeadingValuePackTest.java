package net.kotek.jdbm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase;

public class BTreeLeadingValuePackTest extends TestCase {

    public static class ByteArraySource {
        byte[] last = new byte[0];
        Random r;

        public ByteArraySource(long seed) {
            r = new Random(seed);
            r.nextBytes(last);
        }

        public byte[] getBytesWithCommonPrefix(int len, int common) {
            if (common > last.length) common = last.length;
            if (common > len) common = len;

            byte[] out = new byte[len];
            System.arraycopy(last, 0, out, 0, common);
            byte[] xtra = new byte[len - common];
            r.nextBytes(xtra);
            System.arraycopy(xtra, 0, out, common, xtra.length);

            last = out;
            return out;
        }

    }

    private void doCompressUncompressTestFor(byte[][] groups) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        //compress
        for (int i = 0; i < groups.length; i++) {
            BTreeNode.leadingValuePackWrite(dos, groups[i], i > 0 ? groups[i - 1] : null, 0);
        }

        byte[] results = baos.toByteArray();

        ByteArrayInputStream bais = new ByteArrayInputStream(results);
        DataInputStream dis = new DataInputStream(bais);

        byte[] previous = null;
        for (int i = 0; i < groups.length; i++) {
            previous = BTreeNode.leadingValuePackRead(dis, previous, 0);
            assertTrue(Arrays.equals(groups[i], previous));
        }

    }

    private byte[][] getIncrementingGroups(int groupCount, long seed, int lenInit, int comInit, int lenIncr, int comIncr) {
        ByteArraySource bap = new ByteArraySource(seed);
        byte[][] groups = new byte[groupCount][];
        for (int i = 0; i < groupCount; i++) {
            groups[i] = bap.getBytesWithCommonPrefix(lenInit, comInit);
            lenInit += lenIncr;
            comInit += comIncr;
        }
        return groups;
    }

    public void testCompDecompEqualLenEqualCommon() throws IOException {
        byte[][] groups = getIncrementingGroups(
                5, // number of groups
                1000, // seed
                50, // starting byte array length
                5, // starting common bytes
                0, // length increment
                0 // common bytes increment
        );

        doCompressUncompressTestFor(groups);
    }

    public void testCompDecompEqualLenIncrCommon() throws IOException {
        byte[][] groups = getIncrementingGroups(
                5, // number of groups
                1000, // seed
                50, // starting byte array length
                5, // starting common bytes
                0, // length increment
                2 // common bytes increment
        );

        doCompressUncompressTestFor(groups);
    }

    public void testCompDecompEqualLenDecrCommon() throws IOException {
        byte[][] groups = getIncrementingGroups(
                5, // number of groups
                1000, // seed
                50, // starting byte array length
                40, // starting common bytes
                0, // length increment
                -2 // common bytes increment
        );

        doCompressUncompressTestFor(groups);
    }

    public void testCompDecompIncrLenEqualCommon() throws IOException {
        byte[][] groups = getIncrementingGroups(
                5, // number of groups
                1000, // seed
                30, // starting byte array length
                25, // starting common bytes
                1, // length increment
                0 // common bytes increment
        );

        doCompressUncompressTestFor(groups);
    }

    public void testCompDecompDecrLenEqualCommon() throws IOException {
        byte[][] groups = getIncrementingGroups(
                5, // number of groups
                1000, // seed
                50, // starting byte array length
                25, // starting common bytes
                -1, // length increment
                0 // common bytes increment
        );

        doCompressUncompressTestFor(groups);
    }

    public void testCompDecompNoCommon() throws IOException {
        byte[][] groups = getIncrementingGroups(
                5, // number of groups
                1000, // seed
                50, // starting byte array length
                0, // starting common bytes
                -1, // length increment
                0 // common bytes increment
        );

        doCompressUncompressTestFor(groups);
    }

    public void testCompDecompNullGroups() throws IOException {
        byte[][] groups = getIncrementingGroups(
                5, // number of groups
                1000, // seed
                50, // starting byte array length
                25, // starting common bytes
                -1, // length increment
                0 // common bytes increment
        );

        groups[2] = null;
        groups[4] = null;

        doCompressUncompressTestFor(groups);
    }

}
