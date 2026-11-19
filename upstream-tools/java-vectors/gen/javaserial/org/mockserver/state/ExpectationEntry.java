package org.mockserver.state;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * STRUCTURAL STAND-IN for vector generation only — NOT the production class.
 *
 * Reproduces the real org.mockserver.state.ExpectationEntry's exact wire-relevant shape (package,
 * class name, serialVersionUID, field set/order, transient "expectation" field, and the custom
 * writeObject/readObject pattern of defaultWriteObject() + writeUTF(json)/readUTF()) so the JDK
 * Object Serialization Stream Protocol bytes it produces — class descriptor, handle numbering,
 * block data framing — are byte-identical to what the real class would produce for equivalent
 * field values. The only difference is the *content* of the JSON string: the real class derives
 * it from Jackson + ExpectationDTO + the full mockserver-core Expectation object graph, which
 * this crate does not vendor; here it is a plain literal. That difference is invisible to the
 * wire protocol (it is opaque UTF-8 payload bytes either way).
 *
 * See mockserver-core/src/main/java/org/mockserver/state/ExpectationEntry.java in MockServer for the real class this mirrors.
 */
public final class ExpectationEntry implements Serializable {

    private static final long serialVersionUID = 2L;

    private transient Object expectation;
    private final int priority;
    private final long created;
    private final String id;
    private int remainingTimes;

    public ExpectationEntry(int priority, long created, String id, int remainingTimes, String json) {
        this.priority = priority;
        this.created = created;
        this.id = id;
        this.remainingTimes = remainingTimes;
        this.expectation = json;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF((String) expectation);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.expectation = in.readUTF();
    }
}
