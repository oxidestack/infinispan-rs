import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import org.mockserver.state.infinispan.VersionedWrapper;
import org.mockserver.state.Blob;
import org.mockserver.state.ExpectationEntry;
import com.fasterxml.jackson.databind.node.NodeSerializationHelper;

/**
 * Real, JDK-produced Object Serialization Stream Protocol vectors, for cross-checking the Rust
 * java_serial codec. Every class here except ExpectationEntry (see its own doc comment: a
 * structural stand-in, same wire shape, no Jackson dependency) is the actual upstream class,
 * compiled/loaded unmodified: VersionedWrapper and Blob from a MockServer checkout
 * (read-only reference), NodeSerialization from the pinned jackson-databind-2.22.1.jar.
 */
public class JavaSerialVectorGen {

    static PrintStream out;

    public static void main(String[] args) throws Exception {
        out = new PrintStream(new FileOutputStream(args[0]));

        emit("string_hello", "hello");
        emit("string_empty", "");
        emit("string_non_ascii", "héllo 中文");

        emit("boxed_long", 42L);
        emit("boxed_long_negative", -123456789L);
        emit("boxed_integer", -7);
        emit("boxed_integer_zero", 0);

        emit("versioned_wrapper_string", new VersionedWrapper<>("hello", 1L));
        emit("versioned_wrapper_null_value", new VersionedWrapper<>(null, 0L));

        emit("blob_default_empty_map", new Blob("key1", new byte[]{1, 2, 3, 4}));
        Map<String, String> meta = new HashMap<>();
        meta.put("contentType", "text/plain");
        emit("blob_with_metadata", new Blob("key2", new byte[]{9, 8, 7}, meta));
        emit("blob_empty_data", new Blob("key3", new byte[0]));

        emit("node_serialization", NodeSerializationHelper.create(new byte[]{0x7b, 0x7d}));
        emit("node_serialization_empty", NodeSerializationHelper.create(new byte[0]));

        emit("expectation_entry_unlimited",
                new ExpectationEntry(0, 1000L, "id-1", -1, "{\"httpRequest\":{\"path\":\"/test\"}}"));
        emit("expectation_entry_limited",
                new ExpectationEntry(10, 2000L, "limited-id", 5, "{\"httpRequest\":{\"path\":\"/limited\"}}"));

        // Two top-level writes of the SAME String instance into one stream: exercises handle
        // back-reference (TC_REFERENCE) instead of re-encoding TC_STRING.
        emitPair("string_backref", "shared-instance", true);
        // Two top-level writes of two DIFFERENT VersionedWrapper instances: exercises class
        // descriptor back-reference (second TC_OBJECT reuses the first's TC_CLASSDESC handle).
        emitPairObjects("versioned_wrapper_classdesc_backref",
                new VersionedWrapper<>("a", 1L), new VersionedWrapper<>("b", 2L));

        out.close();
    }

    static void emit(String label, Object value) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(value);
        }
        emitHex(label, baos.toByteArray());
    }

    static void emitPair(String label, Object value, boolean sameInstance) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(value);
            oos.writeObject(sameInstance ? value : value);
        }
        emitHex(label, baos.toByteArray());
    }

    static void emitPairObjects(String label, Object a, Object b) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(a);
            oos.writeObject(b);
        }
        emitHex(label, baos.toByteArray());
    }

    static void emitHex(String label, byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        out.println(label + "\t" + sb);
    }
}
