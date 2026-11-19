package org.infinispan.commons.io;
public class ByteBufferImpl implements ByteBuffer {
    private final byte[] buf; private final int offset; private final int length;
    private ByteBufferImpl(byte[] buf, int offset, int length) { this.buf = buf; this.offset = offset; this.length = length; }
    public static ByteBufferImpl create(byte[] buf, int offset, int length) { return new ByteBufferImpl(buf, offset, length); }
    public byte[] getBuf() { return buf; }
    public int getOffset() { return offset; }
    public int getLength() { return length; }
}
