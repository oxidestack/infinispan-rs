//! `ObjectOutput.writeObject`/`ObjectInput.readObject` for the values a cache command carries: keys and
//! values are `null`, a `GlobalMarshaller` primitive (`String`, `Boolean`, `Long`, ...), or a *user
//! object*, which MockServer marshals with `JavaSerializationMarshaller` and which the
//! `GlobalMarshaller` wraps in its `ID_UNKNOWN` envelope (`marshall/wrapped_message.rs`).

use std::sync::Arc;

use crate::marshall::error::{Error, Result};
use crate::marshall::global::{self, Value};
use crate::marshall::primitives::PrimitiveValue;
use crate::marshall::wrapped_message::{unwrap_unknown, wrap_unknown};
use crate::topology::wire::{ID_NULL, ObjectInput, ObjectOutput};

const ID_UNKNOWN: u8 = 0x05;

/// A user object as the persistence marshaller wrote it (a Java serialization stream).
///
/// An in-memory value keeps its allocation: [`UserValue::same_instance`] is Java's `==` for a value
/// the local node handed out, while a value that crossed the wire is a fresh instance, exactly like a
/// deserialised object (this is what makes `VersionedWrapper.equals` over a value type without
/// `equals` behave as it does in a cluster, see `cache::equality`).
#[derive(Debug, Clone)]
pub struct UserValue(Arc<Vec<u8>>);

impl UserValue {
    /// Wraps the Java serialization stream of a user object.
    pub fn new(java_serialization_stream: Vec<u8>) -> Self {
        Self(Arc::new(java_serialization_stream))
    }
    /// The Java serialization stream.
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }
    /// Java reference equality.
    pub fn same_instance(&self, other: &UserValue) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// Content equality, for tests; Java `equals` semantics live in `cache::equality`.
impl PartialEq for UserValue {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

/// A key or value in the cache: null, a primitive the marshaller writes itself, or a user object.
#[derive(Debug, Clone, PartialEq)]
pub enum CacheObject {
    /// Java's `null`.
    Null,
    /// A string, number, boolean or array of them.
    Primitive(PrimitiveValue),
    /// Any other object, as its Java serialization stream.
    User(UserValue),
}

impl CacheObject {
    /// A string value.
    pub fn string(s: &str) -> Self {
        CacheObject::Primitive(PrimitiveValue::Str(s.to_string()))
    }
    /// Whether this is `null`.
    pub fn is_null(&self) -> bool {
        matches!(self, CacheObject::Null)
    }
    /// The string, if this is one.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            CacheObject::Primitive(PrimitiveValue::Str(s)) => Some(s),
            _ => None,
        }
    }
}

impl ObjectOutput {
    /// `writeObject(Object)` for a cache key or value.
    pub fn write_cache_object(&mut self, o: &CacheObject) {
        match o {
            CacheObject::Null => self.write_null(),
            CacheObject::Primitive(p) => global::write_nullable(&mut self.w, &Value::Primitive(p.clone())),
            CacheObject::User(u) => self.w.write_raw(&wrap_unknown(u.bytes())),
        }
    }

    /// `UnsignedNumeric.writeUnsignedInt`: 7-bit groups, least significant first.
    pub fn write_unsigned_int(&mut self, mut v: u32) {
        while v & !0x7f != 0 {
            self.w.write_unsigned_byte((v & 0x7f) as u8 | 0x80);
            v >>= 7;
        }
        self.w.write_unsigned_byte(v as u8);
    }
}

impl ObjectInput<'_> {
    /// `readObject()` for a cache key or value.
    pub fn read_cache_object(&mut self) -> Result<CacheObject> {
        let mut peek = self.r;
        let tag = peek.read_unsigned_byte()?;
        match tag {
            ID_NULL => {
                self.r.read_unsigned_byte()?;
                Ok(CacheObject::Null)
            }
            ID_UNKNOWN => {
                let len = peek.read_int()?;
                if len < 0 {
                    return Err(Error::Malformed("negative user object length".into()));
                }
                let whole = self.r.read_raw(1 + 4 + len as usize)?;
                Ok(CacheObject::User(UserValue::new(unwrap_unknown(whole)?)))
            }
            _ => match global::read_nullable(&mut self.r)? {
                Value::Primitive(p) => Ok(CacheObject::Primitive(p)),
                other => Err(Error::Malformed(format!("unsupported cache object {other:?}"))),
            },
        }
    }

    /// `UnsignedNumeric.readUnsignedInt`.
    pub fn read_unsigned_int(&mut self) -> Result<u32> {
        let mut result = 0u32;
        let mut shift = 0;
        loop {
            let b = self.r.read_unsigned_byte()?;
            result |= ((b & 0x7f) as u32) << shift;
            if b & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift > 28 {
                return Err(Error::Malformed("unsigned int too long".into()));
            }
        }
    }
}

/// The externalizer id of `ImmortalCacheValue`.
pub const ID_IMMORTAL_VALUE: u8 = 11;
/// The externalizer id of `MortalCacheValue`.
pub const ID_MORTAL_VALUE: u8 = 12;

/// `ImmortalCacheValue` / `MortalCacheValue` (`container/entries/*CacheValue.java`, externalizers
/// ids 11 and 12): what a `ClusteredGetCommand` returns. `PrivateMetadata` is `null`.
#[derive(Debug, Clone, PartialEq)]
pub struct InternalCacheValue {
    /// The value.
    pub value: CacheObject,
    /// `(created, lifespan)` of a `MortalCacheValue`, milliseconds.
    pub mortal: Option<(i64, i64)>,
}

impl ObjectOutput {
    /// `UnsignedNumeric.writeUnsignedLong`.
    pub fn write_unsigned_long(&mut self, mut v: u64) {
        while v & !0x7f != 0 {
            self.w.write_unsigned_byte((v & 0x7f) as u8 | 0x80);
            v >>= 7;
        }
        self.w.write_unsigned_byte(v as u8);
    }

    /// Writes `v` as an `ImmortalCacheValue` or a `MortalCacheValue`.
    pub fn write_internal_cache_value(&mut self, v: &InternalCacheValue) {
        self.write_internal_header(if v.mortal.is_some() { ID_MORTAL_VALUE } else { ID_IMMORTAL_VALUE });
        self.write_cache_object(&v.value);
        self.write_null(); // PrivateMetadata
        if let Some((created, lifespan)) = v.mortal {
            self.write_unsigned_long(created as u64);
            self.w.write_long(lifespan);
        }
    }
}

impl ObjectInput<'_> {
    /// `UnsignedNumeric.readUnsignedLong`.
    pub fn read_unsigned_long(&mut self) -> Result<u64> {
        let mut result = 0u64;
        let mut shift = 0;
        loop {
            let b = self.r.read_unsigned_byte()?;
            result |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift > 63 {
                return Err(Error::Malformed("unsigned long too long".into()));
            }
        }
    }

    /// Reads an `ImmortalCacheValue` or a `MortalCacheValue`.
    pub fn read_internal_cache_value(&mut self) -> Result<InternalCacheValue> {
        if self.r.read_unsigned_byte()? != crate::topology::wire::ID_INTERNAL {
            return Err(Error::Malformed("InternalCacheValue is not an internal object".into()));
        }
        let id = self.r.read_unsigned_byte()?;
        if id != ID_IMMORTAL_VALUE && id != ID_MORTAL_VALUE {
            return Err(Error::UnknownTag { what: "InternalCacheValue externalizer id", value: id as i64 });
        }
        let value = self.read_cache_object()?;
        if self.r.read_unsigned_byte()? != ID_NULL {
            return Err(Error::Malformed("PrivateMetadata is not modelled".into()));
        }
        let mortal = if id == ID_MORTAL_VALUE { Some((self.read_unsigned_long()? as i64, self.r.read_long()?)) } else { None };
        Ok(InternalCacheValue { value, mortal })
    }
}
