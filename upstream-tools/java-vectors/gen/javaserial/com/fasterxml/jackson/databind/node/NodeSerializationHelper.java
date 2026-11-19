package com.fasterxml.jackson.databind.node;

// NodeSerialization's constructor is package-private-visible only (class itself has no public
// modifier); this helper lives in the same package purely to construct real instances for vector
// generation.
public class NodeSerializationHelper {
    public static NodeSerialization create(byte[] json) {
        return new NodeSerialization(json);
    }
}
