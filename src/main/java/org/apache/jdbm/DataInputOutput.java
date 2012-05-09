package org.apache.jdbm;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Utility class which implements DataInput and DataOutput on top of byte[] buffer
 * with minimal overhead
 *
 * @author Jan Kotek
 */
class DataInputOutput implements DataInput, DataOutput, ObjectInput, ObjectOutput {

    private int pos = 0;
    private int count = 0;
    private byte[] buf;


    public DataInputOutput() {
        buf = new byte[8];
    }

    public DataInputOutput(byte[] data) {
        buf = data;
        count = data.length;
    }

    public byte[] getBuf() {
        return buf;
    }

    public int getPos() {
        return pos;
    }


    public void reset() {
        pos = 0;
        count = 0;
    }


    public void resetForReading() {
        count = pos;
        pos = 0;
    }

    public void reset(byte[] b) {
        pos = 0;
        buf = b;
        count = b.length;
    }

    public byte[] toByteArray() {
        byte[] d = new byte[pos];
        System.arraycopy(buf, 0, d, 0, pos);
        return d;
    }

    public int available() {
        return count - pos;
    }


    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        System.arraycopy(buf, pos, b, off, len);
        pos += len;
    }

    public int skipBytes(int n) throws IOException {
        pos += n;
        return n;
    }

    public boolean readBoolean() throws IOException {
        return buf[pos++] == 1;
    }

    public byte readByte() throws IOException {
        return buf[pos++];
    }

    public int readUnsignedByte() throws IOException {
        return buf[pos++] & 0xff;
    }

    public short readShort() throws IOException {
        return (short)
                (((short) (buf[pos++] & 0xff) << 8) |
                        ((short) (buf[pos++] & 0xff) << 0));

    }

    public int readUnsignedShort() throws IOException {
        return (((int) (buf[pos++] & 0xff) << 8) |
                ((int) (buf[pos++] & 0xff) << 0));
    }

    public char readChar() throws IOException {
        return (char) readInt();
    }

    public int readInt() throws IOException {
        return
                (((buf[pos++] & 0xff) << 24) |
                        ((buf[pos++] & 0xff) << 16) |
                        ((buf[pos++] & 0xff) << 8) |
                        ((buf[pos++] & 0xff) << 0));

    }

    public long readLong() throws IOException {
        return
                (((long) (buf[pos++] & 0xff) << 56) |
                        ((long) (buf[pos++] & 0xff) << 48) |
                        ((long) (buf[pos++] & 0xff) << 40) |
                        ((long) (buf[pos++] & 0xff) << 32) |
                        ((long) (buf[pos++] & 0xff) << 24) |
                        ((long) (buf[pos++] & 0xff) << 16) |
                        ((long) (buf[pos++] & 0xff) << 8) |
                        ((long) (buf[pos++] & 0xff) << 0));

    }

    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    public String readLine() throws IOException {
        return readUTF();
    }

    public String readUTF() throws IOException {
        return Serialization.deserializeString(this);
    }

    /**
     * make sure there will be enought space in buffer to write N bytes
     */
    private void ensureAvail(int n) {
        if (pos + n >= buf.length) {
            int newSize = Math.max(pos + n, buf.length * 2);
            buf = Arrays.copyOf(buf, newSize);
        }
    }



    public void write(int b) throws IOException {
        ensureAvail(1);
        buf[pos++] = (byte) b;
    }

    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        ensureAvail(len);
        System.arraycopy(b, off, buf, pos, len);
        pos += len;
    }

    public void writeBoolean(boolean v) throws IOException {
        ensureAvail(1);
        buf[pos++] = (byte) (v ? 1 : 0);
    }

    public void writeByte(int v) throws IOException {
        ensureAvail(1);
        buf[pos++] = (byte) (v);
    }

    public void writeShort(int v) throws IOException {
        ensureAvail(2);
        buf[pos++] = (byte) (0xff & (v >> 8));
        buf[pos++] = (byte) (0xff & (v >> 0));

    }

    public void writeChar(int v) throws IOException {
        writeInt(v);
    }

    public void writeInt(int v) throws IOException {
        ensureAvail(4);
        buf[pos++] = (byte) (0xff & (v >> 24));
        buf[pos++] = (byte) (0xff & (v >> 16));
        buf[pos++] = (byte) (0xff & (v >> 8));
        buf[pos++] = (byte) (0xff & (v >> 0));

    }

    public void writeLong(long v) throws IOException {
        ensureAvail(8);
        buf[pos++] = (byte) (0xff & (v >> 56));
        buf[pos++] = (byte) (0xff & (v >> 48));
        buf[pos++] = (byte) (0xff & (v >> 40));
        buf[pos++] = (byte) (0xff & (v >> 32));
        buf[pos++] = (byte) (0xff & (v >> 24));
        buf[pos++] = (byte) (0xff & (v >> 16));
        buf[pos++] = (byte) (0xff & (v >> 8));
        buf[pos++] = (byte) (0xff & (v >> 0));
    }

    public void writeFloat(float v) throws IOException {
        ensureAvail(4);
        writeInt(Float.floatToIntBits(v));
    }

    public void writeDouble(double v) throws IOException {
        ensureAvail(8);
        writeLong(Double.doubleToLongBits(v));
    }

    public void writeBytes(String s) throws IOException {
        writeUTF(s);
    }

    public void writeChars(String s) throws IOException {
        writeUTF(s);
    }

    public void writeUTF(String s) throws IOException {
        Serialization.serializeString(this, s);
    }

    /** helper method to write data directly from PageIo*/
    public void writeFromByteBuffer(ByteBuffer b, int offset, int length) {
        ensureAvail(length);
        b.position(offset);
        b.get(buf,pos,length);
        pos+=length;
    }


    //temp var used for Externalizable
    SerialClassInfo serializer;
    //temp var used for Externalizable
    Serialization.FastArrayList objectStack;

    public Object readObject() throws ClassNotFoundException, IOException {
        //is here just to implement ObjectInput
        //Fake method which reads data from serializer.
        //We could probably implement separate wrapper for this, but I want to safe class space
        return serializer.deserialize(this, objectStack);
    }

    public int read() throws IOException {
        //is here just to implement ObjectInput
        return readUnsignedByte();
    }

    public int read(byte[] b) throws IOException {
        //is here just to implement ObjectInput
        readFully(b);
        return b.length;
    }

    public int read(byte[] b, int off, int len) throws IOException {
        //is here just to implement ObjectInput
        readFully(b,off,len);
        return len;
    }

    public long skip(long n) throws IOException {
        //is here just to implement ObjectInput
        pos += n;
        return n;
    }

    public void close() throws IOException {
        //is here just to implement ObjectInput
        //do nothing
    }

    public void writeObject(Object obj) throws IOException {
        //is here just to implement ObjectOutput
        serializer.serialize(this,obj,objectStack);
    }


    public void flush() throws IOException {
        //is here just to implement ObjectOutput
        //do nothing
    }

}
