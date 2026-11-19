package org.infinispan.commons.io;
// Compile-only stub: our vector generator uses BytesObjectOutput.toBytes(), never toByteBuffer().
public interface ByteBuffer {
    byte[] getBuf();
    int getOffset();
    int getLength();
}
