//! `org.mockserver.state.infinispan.VersionedWrapper` (serialVersionUID 1, fields `long version`,
//! `Object value`) as a Java serialization stream, and Java's `VersionedWrapper.equals`
//! (`VersionedWrapper.java:44-54`: same class, same version, `Objects.equals(value, that.value)`).

use std::rc::Rc;

use crate::commands::object::{CacheObject, UserValue};
use crate::marshall::error::{Error, Result};
use crate::marshall::java_serial::{
    ClassDesc, FieldDesc, FieldValue, JavaValue, ObjectLevel, SC_SERIALIZABLE, decode_one, encode_one,
};
use crate::marshall::primitives::PrimitiveValue;

/// The class of MockServer's versioned cache value.
pub const CLASS_NAME: &str = "org.mockserver.state.infinispan.VersionedWrapper";

/// MockServer's `VersionedWrapper`: a value with the version its writer read.
#[derive(Debug, Clone, PartialEq)]
pub struct VersionedWrapper {
    /// The version.
    pub version: i64,
    /// The wrapped value, as a Java object.
    pub value: JavaValue,
}

fn class_desc() -> Rc<ClassDesc> {
    Rc::new(ClassDesc {
        name: CLASS_NAME.into(),
        serial_version_uid: 1,
        flags: SC_SERIALIZABLE,
        fields: vec![FieldDesc::primitive(b'J', "version"), FieldDesc::object("value", "Ljava/lang/Object;")],
        super_desc: None,
    })
}

/// A `java.lang.Integer` as Java serialization writes it: `Integer` (serialVersionUID
/// 1360826667806852920, field `int value`) under `Number` (serialVersionUID -8742448824652078965, no
/// fields).
pub fn java_integer(value: i32) -> JavaValue {
    let number = Rc::new(ClassDesc {
        name: "java.lang.Number".into(),
        serial_version_uid: -8742448824652078965,
        flags: SC_SERIALIZABLE,
        fields: vec![],
        super_desc: None,
    });
    let integer = Rc::new(ClassDesc {
        name: "java.lang.Integer".into(),
        serial_version_uid: 1360826667806852920,
        flags: SC_SERIALIZABLE,
        fields: vec![FieldDesc::primitive(b'I', "value")],
        super_desc: Some(number),
    });
    JavaValue::Object {
        class: integer,
        levels: vec![
            ObjectLevel { fields: vec![], annotation: vec![] },
            ObjectLevel { fields: vec![FieldValue::Int(value)], annotation: vec![] },
        ],
    }
}

impl VersionedWrapper {
    /// Parse a `VersionedWrapper` stream.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        match decode_one(bytes)? {
            JavaValue::Object { class, levels } if class.name == CLASS_NAME => {
                let fields = &levels.first().ok_or_else(|| Error::Malformed("VersionedWrapper without fields".into()))?.fields;
                match fields.as_slice() {
                    [FieldValue::Long(version), FieldValue::Obj(value)] => Ok(Self { version: *version, value: (**value).clone() }),
                    other => Err(Error::Malformed(format!("unexpected VersionedWrapper fields {other:?}"))),
                }
            }
            other => Err(Error::Malformed(format!("not a VersionedWrapper: {other:?}"))),
        }
    }

    /// The wrapper as a Java serialization stream.
    pub fn encode(&self) -> UserValue {
        let value = JavaValue::Object {
            class: class_desc(),
            levels: vec![ObjectLevel {
                fields: vec![FieldValue::Long(self.version), FieldValue::Obj(Box::new(self.value.clone()))],
                annotation: vec![],
            }],
        };
        UserValue::new(encode_one(&value))
    }

    /// `new VersionedWrapper<>(value, version)` for a captured/cache value: a `String`, an `Integer`
    /// (MockServer's `sharedTimesCounters`), `null`, or a serialised user object.
    pub fn encode_object(value: &CacheObject, version: i64) -> Result<UserValue> {
        let inner = match value {
            CacheObject::Null => JavaValue::Null,
            CacheObject::Primitive(PrimitiveValue::Str(s)) => JavaValue::Str(s.clone()),
            CacheObject::Primitive(PrimitiveValue::IntObj(i)) => java_integer(*i),
            CacheObject::User(u) => decode_one(u.bytes())?,
            CacheObject::Primitive(other) => return Err(Error::Malformed(format!("cannot nest {other:?} in a VersionedWrapper"))),
        };
        Ok(Self { version, value: inner }.encode())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `new ObjectOutputStream(out).writeObject(Integer.valueOf(5))`, as the JDK writes it.
    const INTEGER_5: &str = "aced0005737200116a6176612e6c616e672e496e746567657212e2a0a4f781873802000149000576616c7565787200106a6176612e6c616e672e4e756d62657286ac951d0b94e08b020000787000000005";

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }

    #[test]
    fn an_integer_encodes_as_the_jdk_writes_it() {
        assert_eq!(encode_one(&java_integer(5)), hex(INTEGER_5));
        assert_eq!(decode_one(&hex(INTEGER_5)).unwrap(), java_integer(5));
    }

    #[test]
    fn a_captured_integer_nests_in_a_versioned_wrapper() {
        let wrapped = VersionedWrapper::encode_object(&CacheObject::Primitive(PrimitiveValue::IntObj(7)), 3).unwrap();
        let read = VersionedWrapper::decode(wrapped.bytes()).unwrap();
        assert_eq!(read, VersionedWrapper { version: 3, value: java_integer(7) });
    }
}
