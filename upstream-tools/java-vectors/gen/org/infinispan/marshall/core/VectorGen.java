package org.infinispan.marshall.core;

import java.io.PrintStream;
import java.io.FileOutputStream;
import java.util.Map;

/**
 * Emits real, upstream-produced byte vectors for cross-checking the Rust port of
 * Primitives.java / BytesObjectOutput.java / BytesObjectInput.java (Infinispan 14.0.35.Final).
 * Compiled and run directly against the unmodified upstream sources (see infinispan-rs
 * tests/vectors/README for the exact compile command); only the GlobalMarshaller field required
 * for BytesObjectOutput/BytesObjectInput to compile is a compile-only stub never exercised here
 * (no vector below calls writeObject(Object)/readObject()).
 *
 * Output format: one line per vector, tab-separated: <label>\t<hex bytes>
 */
public class VectorGen {

    static PrintStream out;

    public static void main(String[] args) throws Exception {
        out = new PrintStream(new FileOutputStream(args[0]));

        // --- raw primitive writes (BytesObjectOutput) ---
        raw("writeBoolean_true", o -> o.writeBoolean(true));
        raw("writeBoolean_false", o -> o.writeBoolean(false));
        raw("writeByte_0", o -> o.writeByte(0));
        raw("writeByte_127", o -> o.writeByte(127));
        raw("writeByte_-128", o -> o.writeByte(-128));
        raw("writeByte_255", o -> o.writeByte(255));
        raw("writeShort_0", o -> o.writeShort(0));
        raw("writeShort_32767", o -> o.writeShort(32767));
        raw("writeShort_-32768", o -> o.writeShort(-32768));
        raw("writeChar_A", o -> o.writeChar('A'));
        raw("writeChar_unicode", o -> o.writeChar(0x1234));
        raw("writeInt_0", o -> o.writeInt(0));
        raw("writeInt_1", o -> o.writeInt(1));
        raw("writeInt_-1", o -> o.writeInt(-1));
        raw("writeInt_max", o -> o.writeInt(Integer.MAX_VALUE));
        raw("writeInt_min", o -> o.writeInt(Integer.MIN_VALUE));
        raw("writeLong_0", o -> o.writeLong(0L));
        raw("writeLong_max", o -> o.writeLong(Long.MAX_VALUE));
        raw("writeLong_min", o -> o.writeLong(Long.MIN_VALUE));
        raw("writeFloat_1.5", o -> o.writeFloat(1.5f));
        raw("writeFloat_-0.0", o -> o.writeFloat(-0.0f));
        raw("writeDouble_1.5", o -> o.writeDouble(1.5d));
        raw("writeDouble_nan", o -> o.writeDouble(Double.NaN));

        // --- string encoding (BytesObjectOutput.writeString) ---
        raw("writeString_empty", o -> o.writeString(""));
        raw("writeString_ascii_short", o -> o.writeString("hello"));
        raw("writeString_ascii_63", o -> o.writeString("x".repeat(63)));
        raw("writeString_ascii_64_forces_utf", o -> o.writeString("x".repeat(64)));
        raw("writeString_non_ascii", o -> o.writeString("hélloé"));
        raw("writeString_multibyte", o -> o.writeString("\u0000߿ࠀ￿"));

        // --- writeUTF (modified UTF-8, used by writeString's "large" branch too) ---
        raw("writeUTF_empty", o -> o.writeUTF(""));
        raw("writeUTF_ascii", o -> o.writeUTF("hello world"));
        raw("writeUTF_non_ascii", o -> o.writeUTF("hélloé 中文"));
        raw("writeUTF_null_char", o -> o.writeUTF("a\u0000b"));

        // --- Primitives.PRIMITIVES dispatch: writePrimitive(obj, out, id) ---
        prim("Boolean_true", Boolean.TRUE);
        prim("Boolean_false", Boolean.FALSE);
        prim("Byte_42", (byte) 42);
        prim("Character_Z", 'Z');
        prim("Double_pi", 3.14159d);
        prim("Float_pi", 3.14159f);
        prim("Integer_12345", 12345);
        prim("Integer_negative", -98765);
        prim("Long_big", 123456789012345L);
        prim("Short_300", (short) 300);
        prim("String_hello", "hello, primitives");

        // --- byte[] / primitive arrays: empty, small, at SMALL_ARRAY_MAX boundary (256), and
        //     just past it (257, MEDIUM branch) ---
        prim("byteArray_empty", new byte[0]);
        prim("byteArray_small3", new byte[]{1, 2, 3});
        prim("byteArray_small256", mkBytes(256));
        prim("byteArray_medium257", mkBytes(257));

        prim("booleanArray_empty", new boolean[0]);
        prim("booleanArray_9", new boolean[]{true, false, true, true, false, false, true, false, true});

        prim("charArray_3", new char[]{'a', 'b', 'c'});
        prim("doubleArray_2", new double[]{1.5, -2.5});
        prim("floatArray_2", new float[]{1.5f, -2.5f});
        prim("intArray_3", new int[]{1, -2, 3});
        prim("longArray_2", new long[]{1L, -2L});
        prim("shortArray_2", new short[]{(short) 1, (short) -2});

        out.close();
    }

    static byte[] mkBytes(int n) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) b[i] = (byte) (i % 251);
        return b;
    }

    interface RawOp { void apply(BytesObjectOutput o) throws Exception; }

    static void raw(String label, RawOp op) throws Exception {
        BytesObjectOutput o = new BytesObjectOutput(64, null);
        op.apply(o);
        emit(label, o.toBytes());
    }

    static void prim(String label, Object value) throws Exception {
        int id = Primitives.PRIMITIVES.get(value.getClass());
        BytesObjectOutput o = new BytesObjectOutput(1024, null);
        Primitives.writePrimitive(value, o, id);
        emit(label, o.toBytes());
    }

    static void emit(String label, byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        out.println(label + "\t" + sb);
    }
}
