//! Port of `GlobalMarshaller`'s type-byte envelope
//! (`core/src/main/java/org/infinispan/marshall/core/GlobalMarshaller.java`, 14.0.35.Final):
//! `writeNullableObject`/`readNullableObject`, `writeNonNullableObject`/`readNonNullableObject`
//! dispatch, and the `ID_ARRAY` flag/length framing (`writeArray`/`readArray`, `:434-570` /
//! `:741-839`).
//!
//! **Scope note:** `ID_INTERNAL`/`ID_EXTERNAL`/`ID_ANNOTATED`/`ID_CLASS`/`ID_LAMBDA`
//! dispatch needs a populated externalizer registry — concretely, Rust types for the ~30
//! internal command/topology/response classes `Ids.java` enumerates. None of those are
//! modelled here (the `commands` module ports the ones a REPL_SYNC cache needs). [`Value`] therefore only
//! models `ID_NULL`, `ID_PRIMITIVE` and `ID_ARRAY` (of primitive-dispatchable elements — the only
//! array shape that needs no registry); `ID_LAMBDA`'s wire format is ported separately in
//! [`super::lambda`], and the `ID_UNKNOWN` "user value" path is ported separately in
//! [`super::wrapped_message`] (both do not depend on the externalizer registry either). Reading an
//! `ID_INTERNAL`/`ID_EXTERNAL`/`ID_ANNOTATED`/`ID_CLASS` tag returns
//! [`Error::UnknownTag`] rather than silently misinterpreting the stream.

use super::error::{Error, Result};
use super::primitives::{ByteReader, ByteWriter, PrimitiveValue};

// GlobalMarshaller.java type-byte constants (`:59-103`).
const ID_NULL: u8 = 0x00;
const ID_PRIMITIVE: u8 = 0x01;
const ID_INTERNAL: u8 = 0x02;
const ID_EXTERNAL: u8 = 0x03;
const ID_ANNOTATED: u8 = 0x04;
/// `GlobalMarshaller.ID_UNKNOWN`: an object the user marshaller wrote.
pub const ID_UNKNOWN: u8 = 0x05;
const ID_ARRAY: u8 = 0x06;
const ID_CLASS: u8 = 0x07;
/// `GlobalMarshaller.ID_LAMBDA`: a serialized lambda.
pub const ID_LAMBDA: u8 = 0x08;

const TYPE_MASK: u8 = 0x07;
const ARRAY_SIZE_MASK: u8 = 0xC0;
const FLAG_SINGLE_TYPE: u8 = 0x08;
const FLAG_COMPONENT_TYPE_MATCH: u8 = 0x10;
const FLAG_ALL_NULL: u8 = 0x20;
const FLAG_ARRAY_EMPTY: u8 = 0x00;
const FLAG_ARRAY_SMALL: u8 = 0x40;
const FLAG_ARRAY_MEDIUM: u8 = 0x80;
const FLAG_ARRAY_LARGE: u8 = 0xC0;

const SMALL_ARRAY_MIN: usize = 1;
const SMALL_ARRAY_MAX: usize = 0x100;
const MEDIUM_ARRAY_MIN: usize = 0x101;
const MEDIUM_ARRAY_MAX: usize = 0x10100;

/// What this crate's envelope can currently carry end to end — see the module scope note.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Java's `null`.
    Null,
    /// A primitive, string or primitive array.
    Primitive(PrimitiveValue),
    /// `ID_ARRAY` of elements that are themselves envelope [`Value`]s (`None` for a null
    /// element). Since a `Value` has no separate "declared component type" from its elements'
    /// own runtime type (unlike Java's reflective `Class<?>`), this port always takes
    /// `FLAG_COMPONENT_TYPE_MATCH` whenever every non-null element shares one
    /// `PrimitiveValue` discriminant — the common case, and the only one that needs no
    /// externalizer/class-identifier registry.
    Array(Vec<Option<Value>>),
}

/// `GlobalMarshaller.writeNullableObject`: `ID_NULL` for null, else the value with its type byte.
pub fn write_nullable(w: &mut ByteWriter, value: &Value) {
    match value {
        Value::Null => w.write_unsigned_byte(ID_NULL),
        other => write_non_nullable(w, other),
    }
}

fn write_non_nullable(w: &mut ByteWriter, value: &Value) {
    match value {
        Value::Null => unreachable!("Value::Null handled by write_nullable"),
        Value::Primitive(p) => {
            w.write_unsigned_byte(ID_PRIMITIVE);
            w.write_primitive(p);
        }
        Value::Array(elements) => write_array(w, elements),
    }
}

/// `GlobalMarshaller.readNullableObject`.
pub fn read_nullable(r: &mut ByteReader) -> Result<Value> {
    let tag = r.read_unsigned_byte()?;
    if tag == ID_NULL {
        return Ok(Value::Null);
    }
    read_non_nullable(r, tag)
}

fn read_non_nullable(r: &mut ByteReader, tag: u8) -> Result<Value> {
    match tag {
        ID_PRIMITIVE => Ok(Value::Primitive(r.read_primitive()?)),
        ID_ARRAY => read_array(r),
        ID_INTERNAL | ID_EXTERNAL | ID_ANNOTATED | ID_CLASS | ID_UNKNOWN | ID_LAMBDA => {
            Err(Error::UnknownTag {
                what: "GlobalMarshaller type byte (not yet ported — needs the externalizer registry, see module docs)",
                value: tag as i64,
            })
        }
        other => Err(Error::UnknownTag {
            what: "GlobalMarshaller type byte",
            value: other as i64,
        }),
    }
}

/// `GlobalMarshaller.writeArray` (`:434-570`), restricted to the primitive-dispatchable /
/// component-type-match / heterogeneous-recursive cases described in the module doc.
fn write_array(w: &mut ByteWriter, elements: &[Option<Value>]) {
    w.write_unsigned_byte(ID_ARRAY);
    let len = elements.len();

    if len == 0 {
        w.write_unsigned_byte(FLAG_ARRAY_EMPTY | ID_PRIMITIVE);
        write_array_length(w, len);
        return;
    }

    let single_type = single_primitive_id(elements);

    let mut flags = size_flag(len);
    match single_type {
        Some(_) => {
            flags |= FLAG_SINGLE_TYPE | FLAG_COMPONENT_TYPE_MATCH;
        }
        None if elements.iter().all(|e| e.is_none()) => {
            flags |= FLAG_SINGLE_TYPE | FLAG_ALL_NULL;
        }
        None => {} // heterogeneous: recurse per element below
    }

    // Component-type byte: we have no registry, so every declared-component path we can take
    // ends up at ID_PRIMITIVE (component type match) — matches upstream's own behaviour for
    // String/boxed-primitive arrays, which are never in the internal/external registries either.
    w.write_unsigned_byte(flags | ID_PRIMITIVE);
    write_array_length(w, len);

    match single_type {
        Some(id) => {
            w.write_unsigned_byte(ID_PRIMITIVE);
            w.write_unsigned_byte(id);
            for e in elements {
                match e {
                    Some(Value::Primitive(p)) => w.write_raw_primitive(p),
                    _ => unreachable!("single_primitive_id guarantees every element matches"),
                }
            }
        }
        None if elements.iter().all(|e| e.is_none()) => {
            // FLAG_ALL_NULL: no element bytes at all.
        }
        None => {
            for e in elements {
                match e {
                    None => w.write_unsigned_byte(ID_NULL),
                    Some(v) => write_non_nullable(w, v),
                }
            }
        }
    }
}

fn size_flag(len: usize) -> u8 {
    if len == 0 {
        FLAG_ARRAY_EMPTY
    } else if len <= SMALL_ARRAY_MAX {
        FLAG_ARRAY_SMALL
    } else if len <= MEDIUM_ARRAY_MAX {
        FLAG_ARRAY_MEDIUM
    } else {
        FLAG_ARRAY_LARGE
    }
}

fn write_array_length(w: &mut ByteWriter, len: usize) {
    if len == 0 {
    } else if len <= SMALL_ARRAY_MAX {
        w.write_unsigned_byte((len - SMALL_ARRAY_MIN) as u8);
    } else if len <= MEDIUM_ARRAY_MAX {
        w.write_unsigned_short((len - MEDIUM_ARRAY_MIN) as u16);
    } else {
        w.write_int(len as i32);
    }
}

/// `Some(id)` iff **every** element is non-`None` and `Value::Primitive` with the same
/// `PrimitiveValue` discriminant (`id()`). Matches `GlobalMarshaller.writeArray`'s own
/// `singleType` computation (`:439-467`): a `null` element anywhere — not just a type mismatch —
/// disqualifies the single-type optimisation, unless every element is `null` (handled by the
/// caller as the separate `FLAG_ALL_NULL` case).
fn single_primitive_id(elements: &[Option<Value>]) -> Option<u8> {
    if elements.is_empty() || elements.iter().any(|e| e.is_none()) {
        return None;
    }
    let mut id = None;
    for e in elements {
        let this_id = match e {
            Some(Value::Primitive(p)) => p.id(),
            _ => return None,
        };
        match id {
            None => id = Some(this_id),
            Some(existing) if existing == this_id => {}
            Some(_) => return None,
        }
    }
    id
}

/// `GlobalMarshaller.readArray` (`:741-839`), matching the subset `write_array` produces above.
fn read_array(r: &mut ByteReader) -> Result<Value> {
    let flags = r.read_unsigned_byte()?;
    let component_type = flags & TYPE_MASK;
    if component_type != ID_PRIMITIVE {
        return Err(Error::UnknownTag {
            what: "array component type (only ID_PRIMITIVE component types are ported — see module docs)",
            value: component_type as i64,
        });
    }

    let masked_size = flags & ARRAY_SIZE_MASK;
    let len = match masked_size {
        FLAG_ARRAY_EMPTY => 0,
        FLAG_ARRAY_SMALL => r.read_unsigned_byte()? as usize + SMALL_ARRAY_MIN,
        FLAG_ARRAY_MEDIUM => r.read_unsigned_short()? as usize + MEDIUM_ARRAY_MIN,
        FLAG_ARRAY_LARGE => r.read_int()? as usize,
        _ => unreachable!("ARRAY_SIZE_MASK covers exactly these four values"),
    };

    if (flags & FLAG_ALL_NULL) != 0 {
        return Ok(Value::Array(vec![None; len]));
    }

    let single_type = (flags & FLAG_SINGLE_TYPE) != 0;
    if !single_type {
        let elements = (0..len)
            .map(|_| {
                let tag = r.read_unsigned_byte()?;
                if tag == ID_NULL {
                    Ok(None)
                } else {
                    Ok(Some(read_non_nullable(r, tag)?))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(Value::Array(elements));
    }

    // FLAG_SINGLE_TYPE | FLAG_COMPONENT_TYPE_MATCH, ID_PRIMITIVE component type.
    let elem_tag = r.read_unsigned_byte()?;
    if elem_tag != ID_PRIMITIVE {
        return Err(Error::UnknownTag {
            what: "array single-type element tag (only ID_PRIMITIVE is ported — see module docs)",
            value: elem_tag as i64,
        });
    }
    let primitive_id = r.read_unsigned_byte()?;
    let elements = (0..len)
        .map(|_| {
            Ok(Some(Value::Primitive(
                r.read_raw_primitive(primitive_id)?,
            )))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Value::Array(elements))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(v: &Value) {
        let mut w = ByteWriter::new();
        write_nullable(&mut w, v);
        let bytes = w.into_bytes();
        let mut r = ByteReader::new(&bytes);
        let decoded = read_nullable(&mut r).unwrap();
        assert_eq!(&decoded, v);
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn null_round_trips() {
        round_trip(&Value::Null);
    }

    #[test]
    fn primitive_round_trips() {
        round_trip(&Value::Primitive(PrimitiveValue::IntObj(42)));
        round_trip(&Value::Primitive(PrimitiveValue::Str("hi".into())));
    }

    #[test]
    fn empty_array_round_trips() {
        round_trip(&Value::Array(vec![]));
    }

    #[test]
    fn all_null_array_round_trips() {
        round_trip(&Value::Array(vec![None, None, None]));
    }

    #[test]
    fn single_type_string_array_round_trips() {
        round_trip(&Value::Array(vec![
            Some(Value::Primitive(PrimitiveValue::Str("a".into()))),
            Some(Value::Primitive(PrimitiveValue::Str("b".into()))),
            Some(Value::Primitive(PrimitiveValue::Str("c".into()))),
        ]));
    }

    #[test]
    fn a_null_element_disqualifies_the_single_type_optimisation() {
        // Matches upstream: unlike a plain type mismatch, a null anywhere (short of every
        // element being null) forces the heterogeneous encoding, not just a "mostly single type"
        // one — see `single_primitive_id`'s doc comment.
        round_trip(&Value::Array(vec![
            Some(Value::Primitive(PrimitiveValue::Str("a".into()))),
            None,
            Some(Value::Primitive(PrimitiveValue::Str("c".into()))),
        ]));
    }

    #[test]
    fn single_type_integer_array_round_trips() {
        let elements: Vec<Option<Value>> = (0..300)
            .map(|i| Some(Value::Primitive(PrimitiveValue::IntObj(i))))
            .collect();
        round_trip(&Value::Array(elements)); // crosses the SMALL/MEDIUM boundary at 256
    }

    #[test]
    fn heterogeneous_array_round_trips() {
        round_trip(&Value::Array(vec![
            Some(Value::Primitive(PrimitiveValue::Str("a".into()))),
            Some(Value::Primitive(PrimitiveValue::IntObj(1))),
            None,
        ]));
    }

    #[test]
    fn unregistered_type_byte_is_an_error_not_a_panic() {
        let bytes = [ID_INTERNAL];
        let mut r = ByteReader::new(&bytes);
        assert!(matches!(
            read_nullable(&mut r),
            Err(Error::UnknownTag { .. })
        ));
    }
}
