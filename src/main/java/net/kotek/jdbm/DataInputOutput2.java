///*
//package net.kotek.jdbm;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import java.nio.Buffer;
//import java.nio.ByteBuffer;
//import java.util.Arrays;
//
//*/
///**
// * Utility class which implements DataInput and DataOutput on top of ByteBuffer
// * with minimal overhead
// * This class is not used, is left here in case we would ever need it.
// *
// * @author Jan Kotek
// *//*
//
//class DataInputOutput2 implements DataInput, DataOutput {
//
//    private ByteBuffer buf;
//
//
//    public DataInputOutput2() {
//        buf = ByteBuffer.allocate(8);
//    }
//
//    public DataInputOutput2(ByteBuffer data) {
//        buf = data;
//    }
//
//    public DataInputOutput2(byte[] data) {
//        buf = ByteBuffer.wrap(data);
//    }
//
//
//    public int getPos() {
//        return buf.position();
//    }
//
//
//    public void reset() {
//        buf.rewind();
//    }
//
//
//    public void reset(byte[] b) {
//        buf = ByteBuffer.wrap(b);
//    }
//
//    public void resetForReading() {
//        buf.flip();
//    }
//
//
//    public byte[] toByteArray() {
//        byte[] d = new byte[buf.position()];
//        buf.position(0);
//        buf.get(d); //reading N bytes restores to current position
//
//        return d;
//    }
//
//    public int available() {
//        return buf.remaining();
//    }
//
//
//    public void readFully(byte[] b) throws IOException {
//        readFully(b, 0, b.length);
//    }
//
//    public void readFully(byte[] b, int off, int len) throws IOException {
//        buf.get(b,off,len);
//    }
//
//    public int skipBytes(int n) throws IOException {
//        buf.position(buf.position()+n);
//        return n;
//    }
//
//    public boolean readBoolean() throws IOException {
//        return buf.get()==1;
//    }
//
//    public byte readByte() throws IOException {
//        return buf.get();
//    }
//
//    public int readUnsignedByte() throws IOException {
//        return buf.get() & 0xff;
//    }
//
//    public short readShort() throws IOException {
//        return buf.getShort();
//    }
//
//    public int readUnsignedShort() throws IOException {
//        return (((int) (buf.get() & 0xff) << 8) |
//                ((int) (buf.get() & 0xff) << 0));
//    }
//
//    public char readChar() throws IOException {
//        return (char) readInt();
//    }
//
//    public int readInt() throws IOException {
//        return buf.getInt();
//    }
//
//    public long readLong() throws IOException {
//        return buf.getLong();
//    }
//
//    public float readFloat() throws IOException {
//        return buf.getFloat();
//    }
//
//    public double readDouble() throws IOException {
//        return buf.getDouble();
//    }
//
//    public String readLine() throws IOException {
//        return readUTF();
//    }
//
//    public String readUTF() throws IOException {
//        return Serialization.deserializeString(this);
//    }
//
//    */
///**
//     * make sure there will be enough space in buffer to write N bytes
//     *//*
//
//    private void ensureAvail(int n) {
//        int pos = buf.position();
//        if (pos + n >= buf.limit()) {
//            int newSize = Math.max(pos + n, buf.limit() * 2);
//            byte[] b = new byte[newSize];
//            buf.get(b);
//            buf = ByteBuffer.wrap(b);
//            buf.position(pos);
//        }
//    }
//
//
//    public void write(final int b) throws IOException {
//        ensureAvail(1);
//        buf.put((byte) b);
//    }
//
//    public void write(final byte[] b) throws IOException {
//        write(b, 0, b.length);
//    }
//
//    public void write(final byte[] b, final int off, final int len) throws IOException {
//        ensureAvail(len);
//        buf.put(b,off,len);
//    }
//
//    public void writeBoolean(final boolean v) throws IOException {
//        ensureAvail(1);
//        buf.put((byte) (v?1:0));
//    }
//
//    public void writeByte(final int v) throws IOException {
//        ensureAvail(1);
//        buf.put((byte) v);
//    }
//
//    public void writeShort(final short v) throws IOException {
//        ensureAvail(2);
//        buf.putShort(v);
//    }
//
//    public void writeChar(final int v) throws IOException {
//        writeInt(v);
//    }
//
//    public void writeInt(final int v) throws IOException {
//        ensureAvail(4);
//        buf.putInt(v);
//    }
//
//    public void writeLong(final long v) throws IOException {
//        ensureAvail(8);
//        buf.putLong(v);
//    }
//
//    public void writeFloat(final float v) throws IOException {
//        ensureAvail(4);
//        buf.putFloat(v);
//    }
//
//    public void writeDouble(final double v) throws IOException {
//        ensureAvail(8);
//        buf.putDouble(v);
//    }
//
//    public void writeBytes(String s) throws IOException {
//        writeUTF(s);
//    }
//
//    public void writeChars(String s) throws IOException {
//        writeUTF(s);
//    }
//
//    public void writeUTF(String s) throws IOException {
//        Serialization.serializeString(this, s);
//    }
//
//}
//*/
