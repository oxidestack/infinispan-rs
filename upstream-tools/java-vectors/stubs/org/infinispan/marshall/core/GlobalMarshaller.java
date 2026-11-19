package org.infinispan.marshall.core;
// Compile-only stub: BytesObjectOutput.java/BytesObjectInput.java (real, unmodified upstream
// files) hold a reference to GlobalMarshaller but our vector generator never calls
// writeObject(Object)/readObject(), the only methods that touch this field, so this stub is
// never exercised at runtime.
public class GlobalMarshaller {
    void writeNullableObject(Object obj, BytesObjectOutput out) throws java.io.IOException {
        throw new UnsupportedOperationException("compile-only stub");
    }
    Object readNullableObject(BytesObjectInput in) throws java.io.IOException, ClassNotFoundException {
        throw new UnsupportedOperationException("compile-only stub");
    }
}
