//! The `GlobalMarshaller` `ID_UNKNOWN` ("user value") path
//! (`core/src/main/java/org/infinispan/marshall/core/GlobalMarshaller.java:616-644,893-906`):
//! `0x05 | len32 | WrappedMessage(typeId=MARSHALLABLE_USER_OBJECT, bytes=MarshallableUserObject(
//! bytes=<persistence marshaller output>))`.
//!
//! `WrappedMessage` is ProtoStream 4.6.5.Final
//! (`upstream-protostream/core/src/main/java/org/infinispan/protostream/WrappedMessage.java`);
//! `MarshallableUserObject` is Infinispan commons
//! (`core/src/main/java/org/infinispan/marshall/protostream/impl/MarshallableUserObject.java`).
//! Both are read unmodified from the vendored/pinned sources — see the module doc below for how
//! the exact field numbers were confirmed, not guessed.
//!
//! Field numbers, confirmed by reading `WrappedMessage.java`'s `writeMessage` (`:245-308`) for
//! the "else" branch (a message type, not a boxed primitive) — the branch `MarshallableUserObject`
//! always takes, since it is neither a primitive/String/byte\[\] nor an enum:
//! - `WRAPPED_TYPE_ID = 19` (`WrappedMessage.java:193`), written as `writeUInt32` (`:293`) since
//!   `MarshallableUserObject` has a fixed numeric ProtoStream type id
//!   (`ProtoStreamTypeIds.MARSHALLABLE_USER_OBJECT = 2`, `commons/all/.../ProtoStreamTypeIds.java:27`).
//! - `WRAPPED_MESSAGE = 17` (`WrappedMessage.java:182`), written as `writeBytes` (`:303`) —
//!   the nested message's own encoded bytes.
//! - `MarshallableUserObject` itself has one field, `bytes` at field number 1
//!   (`@ProtoField(1)`, `MarshallableUserObject.java:51`, written at `Marshaller.write:152`).
//!
//! This independently reproduces a worked example byte for byte:
//! `05 | len32 | 98 01 02 | 8A 01 <len> | 0A <len> | AC ED 00 05 …` — `98 01` is the protobuf tag
//! for field 19 wire-type 0 (varint), value `02`; `8A 01` is the tag for field 17 wire-type 2
//! (length-delimited); `0A` is the tag for field 1 wire-type 2; `AC ED 00 05` is the JDK
//! serialization stream magic — see [`super::java_serial`]. Verified against the module's own
//! test below, which decodes that exact structure produced by this port's writer.

use super::error::{Error, Result};
use super::global::ID_UNKNOWN;
use super::primitives::{ByteReader, ByteWriter};

const WRAPPED_TYPE_ID: u32 = 19;
const WRAPPED_MESSAGE: u32 = 17;
const MARSHALLABLE_USER_OBJECT_FIELD: u32 = 1;
const MARSHALLABLE_USER_OBJECT_TYPE_ID: u32 = 2; // ProtoStreamTypeIds.MARSHALLABLE_USER_OBJECT

const WIRETYPE_VARINT: u32 = 0;
const WIRETYPE_LENGTH_DELIMITED: u32 = 2;

fn write_varint(w: &mut ByteWriter, mut v: u64) {
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            w.write_unsigned_byte(byte);
            break;
        } else {
            w.write_unsigned_byte(byte | 0x80);
        }
    }
}

fn read_varint(r: &mut ByteReader) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0;
    loop {
        let b = r.read_unsigned_byte()?;
        result |= ((b & 0x7F) as u64) << shift;
        if (b & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            return Err(Error::Malformed("varint too long".into()));
        }
    }
}

fn write_tag(w: &mut ByteWriter, field_number: u32, wire_type: u32) {
    write_varint(w, ((field_number as u64) << 3) | wire_type as u64);
}

fn write_length_delimited(w: &mut ByteWriter, field_number: u32, bytes: &[u8]) {
    write_tag(w, field_number, WIRETYPE_LENGTH_DELIMITED);
    write_varint(w, bytes.len() as u64);
    w.write_raw(bytes);
}

/// Builds the `MarshallableUserObject`-then-`WrappedMessage` bytes (without the outer
/// `GlobalMarshaller` `0x05 | len32` framing — see [`wrap_unknown`] for the full envelope).
fn build_wrapped_message(persistence_marshaller_output: &[u8]) -> Vec<u8> {
    let mut inner = ByteWriter::new();
    write_length_delimited(&mut inner, MARSHALLABLE_USER_OBJECT_FIELD, persistence_marshaller_output);
    let inner_bytes = inner.into_bytes();

    let mut outer = ByteWriter::new();
    write_tag(&mut outer, WRAPPED_TYPE_ID, WIRETYPE_VARINT);
    write_varint(&mut outer, MARSHALLABLE_USER_OBJECT_TYPE_ID as u64);
    write_length_delimited(&mut outer, WRAPPED_MESSAGE, &inner_bytes);
    outer.into_bytes()
}

/// The full `GlobalMarshaller` `ID_UNKNOWN` envelope: `0x05 | len32(u32 BE) | WrappedMessage`.
/// `persistence_marshaller_output` is what the persistence marshaller (here: always
/// [`super::java_serial::encode_one`], since `JavaSerializationMarshaller` is the only
/// persistence marshaller this crate ports) produced for the user value.
pub fn wrap_unknown(persistence_marshaller_output: &[u8]) -> Vec<u8> {
    let wrapped = build_wrapped_message(persistence_marshaller_output);
    let mut w = ByteWriter::new();
    w.write_unsigned_byte(ID_UNKNOWN);
    w.write_int(wrapped.len() as i32);
    w.write_raw(&wrapped);
    w.into_bytes()
}

/// Parses [`wrap_unknown`]'s output back to the persistence marshaller bytes it wrapped. Returns
/// [`Error::Malformed`] if the `WrappedMessage`/`MarshallableUserObject` framing doesn't match
/// (wrong field numbers/wire types, or a type id other than `MARSHALLABLE_USER_OBJECT`).
pub fn unwrap_unknown(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut r = ByteReader::new(bytes);
    let tag = r.read_unsigned_byte()?;
    if tag != ID_UNKNOWN {
        return Err(Error::UnknownTag {
            what: "GlobalMarshaller type byte (expected ID_UNKNOWN)",
            value: tag as i64,
        });
    }
    let len = r.read_int()? as usize;
    let wrapped = r.read_raw(len)?;
    unwrap_wrapped_message(wrapped)
}

fn unwrap_wrapped_message(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut r = ByteReader::new(bytes);
    let mut type_id: Option<u64> = None;
    let mut message_bytes: Option<Vec<u8>> = None;

    while r.remaining() > 0 {
        let key = read_varint(&mut r)?;
        let field_number = (key >> 3) as u32;
        let wire_type = (key & 0x7) as u32;
        match (field_number, wire_type) {
            (WRAPPED_TYPE_ID, WIRETYPE_VARINT) => {
                type_id = Some(read_varint(&mut r)?);
            }
            (WRAPPED_MESSAGE, WIRETYPE_LENGTH_DELIMITED) => {
                let len = read_varint(&mut r)? as usize;
                message_bytes = Some(r.read_raw(len)?.to_vec());
            }
            (_, WIRETYPE_LENGTH_DELIMITED) => {
                let len = read_varint(&mut r)? as usize;
                r.read_raw(len)?; // skip unknown length-delimited field
            }
            (_, WIRETYPE_VARINT) => {
                read_varint(&mut r)?; // skip unknown varint field
            }
            _ => {
                return Err(Error::Malformed(format!(
                    "unsupported WrappedMessage wire type {wire_type} for field {field_number}"
                )));
            }
        }
    }

    if type_id != Some(MARSHALLABLE_USER_OBJECT_TYPE_ID as u64) {
        return Err(Error::Malformed(format!(
            "WrappedMessage type id {type_id:?} is not MARSHALLABLE_USER_OBJECT ({MARSHALLABLE_USER_OBJECT_TYPE_ID})"
        )));
    }
    let message_bytes = message_bytes
        .ok_or_else(|| Error::Malformed("WrappedMessage missing field 17 (WRAPPED_MESSAGE)".into()))?;

    unwrap_marshallable_user_object(&message_bytes)
}

fn unwrap_marshallable_user_object(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut r = ByteReader::new(bytes);
    let mut user_bytes: Option<Vec<u8>> = None;
    while r.remaining() > 0 {
        let key = read_varint(&mut r)?;
        let field_number = (key >> 3) as u32;
        let wire_type = (key & 0x7) as u32;
        match (field_number, wire_type) {
            (MARSHALLABLE_USER_OBJECT_FIELD, WIRETYPE_LENGTH_DELIMITED) => {
                let len = read_varint(&mut r)? as usize;
                user_bytes = Some(r.read_raw(len)?.to_vec());
            }
            (_, WIRETYPE_LENGTH_DELIMITED) => {
                let len = read_varint(&mut r)? as usize;
                r.read_raw(len)?;
            }
            (_, WIRETYPE_VARINT) => {
                read_varint(&mut r)?;
            }
            _ => {
                return Err(Error::Malformed(format!(
                    "unsupported MarshallableUserObject wire type {wire_type} for field {field_number}"
                )));
            }
        }
    }
    user_bytes.ok_or_else(|| {
        Error::Malformed("MarshallableUserObject missing field 1 (bytes)".into())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marshall::java_serial::{self, JavaValue};

    #[test]
    fn round_trips_a_real_java_serial_payload() {
        let text = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/vectors/java_serial.hex"
        ))
        .unwrap();
        let (_, hex) = text
            .lines()
            .find(|l| l.starts_with("versioned_wrapper_string\t"))
            .unwrap()
            .split_once('\t')
            .unwrap();
        let java_serial_bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();

        let wrapped = wrap_unknown(&java_serial_bytes);
        let unwrapped = unwrap_unknown(&wrapped).unwrap();
        assert_eq!(unwrapped, java_serial_bytes);

        // And the inner bytes are still a valid, decodable Java serialization stream.
        let decoded = java_serial::decode_one(&unwrapped).unwrap();
        assert!(matches!(decoded, JavaValue::Object { .. }));
    }

    /// Reproduces a worked hex example: `05 | len32 | 98 01 02 | 8A 01
    /// <len> | 0A <len> | AC ED 00 05 …`, byte for byte.
    #[test]
    fn matches_the_worked_hex_example_structure() {
        let payload = [0xac, 0xed, 0x00, 0x05, 0xde, 0xad, 0xbe, 0xef]; // stand-in java_serial bytes
        let wrapped = wrap_unknown(&payload);

        assert_eq!(wrapped[0], 0x05, "ID_UNKNOWN");
        let len = i32::from_be_bytes([wrapped[1], wrapped[2], wrapped[3], wrapped[4]]) as usize;
        assert_eq!(len, wrapped.len() - 5, "len32 covers the rest of the buffer");

        let rest = &wrapped[5..];
        assert_eq!(&rest[0..3], &[0x98, 0x01, 0x02], "field 19 varint tag + typeId=2");
        assert_eq!(&rest[3..5], &[0x8A, 0x01], "field 17 length-delimited tag");
        let inner_len = rest[5] as usize; // single-byte varint length for our small payload
        assert_eq!(&rest[6..6 + 1], &[0x0A], "MarshallableUserObject field 1 tag");
        let user_len = rest[7] as usize;
        assert_eq!(user_len, payload.len());
        assert_eq!(&rest[8..8 + user_len], &payload);
        assert_eq!(inner_len, 2 + user_len); // MarshallableUserObject's own tag+len+bytes
    }

    #[test]
    fn rejects_a_wrapped_message_with_a_different_type_id() {
        // A WrappedMessage claiming to wrap something other than MARSHALLABLE_USER_OBJECT must
        // be refused rather than silently misinterpreted.
        let mut inner = ByteWriter::new();
        write_length_delimited(&mut inner, MARSHALLABLE_USER_OBJECT_FIELD, b"payload");
        let mut outer = ByteWriter::new();
        write_tag(&mut outer, WRAPPED_TYPE_ID, WIRETYPE_VARINT);
        write_varint(&mut outer, 999); // wrong type id
        write_length_delimited(&mut outer, WRAPPED_MESSAGE, &inner.into_bytes());

        let mut w = ByteWriter::new();
        w.write_unsigned_byte(ID_UNKNOWN);
        let body = outer.into_bytes();
        w.write_int(body.len() as i32);
        w.write_raw(&body);

        assert!(unwrap_unknown(&w.into_bytes()).is_err());
    }
}
