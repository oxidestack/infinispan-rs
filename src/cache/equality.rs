//! Java's `Object.equals` over the values a MockServer cache holds, which decides every conditional
//! write (`ValueMatcher.MATCH_EXPECTED`: `existingValue.equals(expectedValue)`).
//!
//! A value is a Java serialization stream. `equals` runs on deserialised objects, so two streams are
//! equal when the classes involved define `equals` and agree field by field. A class without `equals`
//! (MockServer's `Blob`, or the payload of a `VersionedWrapper`) is equal only to the very same
//! instance, and a value that crossed the wire is a new instance: see [`UserValue::same_instance`].
//!
//! Classes with an `equals`: `String`, the boxed numbers, `VersionedWrapper` (`VersionedWrapper.java:44-54`:
//! `version == that.version && Objects.equals(value, that.value)`), `ExpectationEntry`
//! (`ExpectationEntry.java:117-129`: priority, created, remainingTimes, id) and Jackson's
//! `NodeSerialization`-backed `ObjectNode` (structural; compared by its serialised bytes).

use crate::commands::object::{CacheObject, UserValue};
use crate::marshall::java_serial::{AnnotationItem, FieldValue, JavaValue, decode_one};

/// `Objects.equals(a, b)` for two cache values.
pub fn java_equals(a: &CacheObject, b: &CacheObject) -> bool {
    match (a, b) {
        (CacheObject::Null, CacheObject::Null) => true,
        (CacheObject::Primitive(x), CacheObject::Primitive(y)) => x == y,
        (CacheObject::User(x), CacheObject::User(y)) => user_equals(x, y),
        _ => false,
    }
}

fn user_equals(a: &UserValue, b: &UserValue) -> bool {
    if a.same_instance(b) {
        return true;
    }
    match (decode_one(a.bytes()), decode_one(b.bytes())) {
        (Ok(x), Ok(y)) => value_equals(&x, &y),
        _ => false,
    }
}

fn field<'a>(class: &crate::marshall::java_serial::ClassDesc, fields: &'a [FieldValue], name: &str) -> Option<&'a FieldValue> {
    class.fields.iter().position(|f| f.name == name).and_then(|i| fields.get(i))
}

/// Equality of two *distinct* deserialised instances.
fn value_equals(a: &JavaValue, b: &JavaValue) -> bool {
    match (a, b) {
        (JavaValue::Null, JavaValue::Null) => true,
        (JavaValue::Str(x), JavaValue::Str(y)) => x == y,
        (JavaValue::Object { class: ca, levels: la }, JavaValue::Object { class: cb, levels: lb }) => {
            if ca.name != cb.name {
                return false;
            }
            match ca.name.as_str() {
                "org.mockserver.state.infinispan.VersionedWrapper" => {
                    let (fa, fb) = (&la[0].fields, &lb[0].fields);
                    fa.first() == fb.first()
                        && match (fa.get(1), fb.get(1)) {
                            (Some(FieldValue::Obj(x)), Some(FieldValue::Obj(y))) => value_equals(x, y),
                            _ => false,
                        }
                }
                "org.mockserver.state.ExpectationEntry" => {
                    let (fa, fb) = (&la[0].fields, &lb[0].fields);
                    ["priority", "created", "remainingTimes"].iter().all(|n| field(ca, fa, n) == field(cb, fb, n))
                        && match (field(ca, fa, "id"), field(cb, fb, "id")) {
                            (Some(FieldValue::Obj(x)), Some(FieldValue::Obj(y))) => value_equals(x, y),
                            _ => false,
                        }
                }
                "java.lang.Long" | "java.lang.Integer" | "java.lang.Short" | "java.lang.Byte" | "java.lang.Boolean" | "java.lang.Character" => {
                    la.last().map(|l| &l.fields) == lb.last().map(|l| &l.fields)
                }
                "com.fasterxml.jackson.databind.node.NodeSerialization" => {
                    let blocks = |l: &Vec<crate::marshall::java_serial::ObjectLevel>| -> Vec<Vec<u8>> {
                        l.iter().flat_map(|lv| lv.annotation.iter().filter_map(|a| match a { AnnotationItem::Block(b) => Some(b.clone()), AnnotationItem::Object(_) => None })).collect()
                    };
                    blocks(la) == blocks(lb)
                }
                // no equals(): identity, and these are two instances
                _ => false,
            }
        }
        _ => false,
    }
}
