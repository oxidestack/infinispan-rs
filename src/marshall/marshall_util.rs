//! Port of `org.infinispan.commons.marshall.MarshallUtil`'s `marshallSize`/`unmarshallSize`
//! varint (`commons/all/.../marshall/MarshallUtil.java:398-450`) plus the generic
//! `marshallString`/`marshallUUID`/`marshallEnum` helpers built on it. Cross-checked byte-for-byte
//! against real vectors produced by the unmodified upstream class — see
//! `tests/vectors/marshall_size.hex`.
//!
//! `marshallSize` is a 6-bit-first varint: negative Java `int` values are all collapsed, on the
//! wire, to a single `0x80` byte (upstream uses this to mean "null-sized", e.g. a `null`
//! collection) — unmarshalling any byte with the high bit set yields [`NULL_VALUE`] regardless of
//! which negative value was written. This is upstream's real, lossy behaviour, not a bug in this
//! port.

use super::error::{Error, Result};
use super::primitives::{ByteReader, ByteWriter};

/// `MarshallUtil.NULL_VALUE`: what `unmarshall_size` returns for any negative-encoded size.
pub const NULL_VALUE: i32 = -1;

/// `MarshallUtil.marshallSize` (`:398-415`).
pub fn marshall_size(w: &mut ByteWriter, value: i32) {
    if value < 0 {
        w.write_unsigned_byte(0x80);
        return;
    }
    if (value & !0x3F) == 0 {
        w.write_unsigned_byte((value & 0x3F) as u8);
        return;
    }
    w.write_unsigned_byte(((value & 0x3F) | 0x40) as u8);
    let mut v = (value as u32) >> 6;
    while (v & !0x7F) != 0 {
        w.write_unsigned_byte(((v & 0x7F) | 0x80) as u8);
        v >>= 7;
    }
    w.write_unsigned_byte(v as u8);
}

/// `MarshallUtil.unmarshallSize` (`:425-442`).
pub fn unmarshall_size(r: &mut ByteReader) -> Result<i32> {
    let b = r.read_byte()? as u8;
    if (b & 0x80) != 0 {
        return Ok(NULL_VALUE);
    }
    let mut i: u32 = (b & 0x3F) as u32;
    if (b & 0x40) == 0 {
        return Ok(i as i32);
    }
    let mut shift = 6;
    loop {
        let b = r.read_byte()? as u8;
        i |= ((b & 0x7F) as u32) << shift;
        shift += 7;
        if (b & 0x80) == 0 {
            break;
        }
    }
    Ok(i as i32)
}

/// `java.io.DataOutput#writeUTF`: JDK-standard 2-byte-length modified UTF-8. Not what
/// `MarshallUtil.marshallString` writes when the `ObjectOutput` is Infinispan's `BytesObjectOutput`
/// (see [`marshall_string`]); kept for `DataOutput` contexts.
pub fn marshall_jdk_utf(w: &mut ByteWriter, s: &str) {
    let encoded = super::primitives::modified_utf8_encode(s);
    w.write_unsigned_short(encoded.len() as u16);
    w.write_raw(&encoded);
}

/// `DataInput.readUTF`: the counterpart of [`marshall_jdk_utf`].
pub fn unmarshall_jdk_utf(r: &mut ByteReader) -> Result<String> {
    let len = r.read_unsigned_short()? as usize;
    let bytes = r.read_raw(len)?;
    super::primitives::modified_utf8_decode(bytes)
}

/// `MarshallUtil.marshallString`/`unmarshallString` (`:328-350`): a `null`-checked
/// `out.writeUTF`/`in.readUTF`. Every `ObjectOutput` in the `GlobalMarshaller` path is a
/// `BytesObjectOutput`, whose `writeUTF` has a 4-byte length (`ByteWriter::write_utf`), not the
/// JDK's 2-byte one. The topology command
/// vectors produced by the real marshaller (`tests/vectors/topology_wire.hex`) showed that the
/// 2-byte form is wrong.
pub fn marshall_string(w: &mut ByteWriter, s: Option<&str>) {
    match s {
        None => w.write_boolean(true),
        Some(s) => {
            w.write_boolean(false);
            w.write_utf(s);
        }
    }
}

/// The counterpart of [`marshall_string`].
pub fn unmarshall_string(r: &mut ByteReader) -> Result<Option<String>> {
    if r.read_boolean()? {
        Ok(None)
    } else {
        Ok(Some(r.read_utf()?))
    }
}

/// `MarshallUtil.marshallUUID`/`unmarshallUUID` (`:148-174`).
pub fn marshall_uuid(w: &mut ByteWriter, uuid: Option<u128>, check_null: bool) {
    if check_null {
        match uuid {
            None => {
                w.write_boolean(true);
                return;
            }
            Some(_) => w.write_boolean(false),
        }
    }
    let u = uuid.expect("uuid must be Some when check_null is false");
    w.write_long((u >> 64) as i64);
    w.write_long(u as i64);
}

/// The counterpart of [`marshall_uuid`].
pub fn unmarshall_uuid(r: &mut ByteReader, check_null: bool) -> Result<Option<u128>> {
    if check_null && r.read_boolean()? {
        return Ok(None);
    }
    let msb = r.read_long()? as u64 as u128;
    let lsb = r.read_long()? as u64 as u128;
    Ok(Some((msb << 64) | lsb))
}

/// `MarshallUtil.marshallEnum`/`unmarshallEnum` (`:444-462`): ordinal as a single byte, `-1` for
/// `null`.
pub fn marshall_enum_ordinal(w: &mut ByteWriter, ordinal: Option<u8>) {
    match ordinal {
        None => w.write_byte(-1),
        Some(o) => w.write_byte(o as i8),
    }
}

/// The counterpart of [`marshall_enum_ordinal`].
pub fn unmarshall_enum_ordinal(r: &mut ByteReader) -> Result<Option<u8>> {
    let b = r.read_byte()?;
    if b == -1 {
        Ok(None)
    } else if b < 0 {
        Err(Error::Malformed(format!("invalid enum ordinal byte {b}")))
    } else {
        Ok(Some(b as u8))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_vectors(path: &str) -> Vec<(i64, Vec<u8>)> {
        let text = std::fs::read_to_string(path).expect("fixture file");
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|line| {
                let (value, hex) = line.split_once('\t').expect("value\\thex");
                (value.parse().unwrap(), hex_decode(hex))
            })
            .collect()
    }

    fn hex_decode(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn matches_real_java_vectors() {
        let vectors = load_vectors(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/vectors/marshall_size.hex"
        ));
        for (value, expected_bytes) in &vectors {
            let mut w = ByteWriter::new();
            marshall_size(&mut w, *value as i32);
            assert_eq!(&w.into_bytes(), expected_bytes, "marshall_size({value})");
        }
    }

    #[test]
    fn unmarshall_matches_real_java_vectors_for_non_negative_inputs() {
        let vectors = load_vectors(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/vectors/marshall_size.hex"
        ));
        for (value, bytes) in &vectors {
            if *value < 0 {
                // Every negative input collapses to NULL_VALUE on the wire — see module docs.
                let mut r = ByteReader::new(bytes);
                assert_eq!(unmarshall_size(&mut r).unwrap(), NULL_VALUE, "value={value}");
                continue;
            }
            let mut r = ByteReader::new(bytes);
            assert_eq!(unmarshall_size(&mut r).unwrap(), *value as i32, "value={value}");
        }
    }

    #[test]
    fn round_trip_property() {
        for v in [0, 1, 63, 64, 4095, 4096, 1_000_000, i32::MAX] {
            let mut w = ByteWriter::new();
            marshall_size(&mut w, v);
            let bytes = w.into_bytes();
            let mut r = ByteReader::new(&bytes);
            assert_eq!(unmarshall_size(&mut r).unwrap(), v);
            assert_eq!(r.remaining(), 0);
        }
    }

    #[test]
    fn string_uuid_enum_round_trip() {
        let mut w = ByteWriter::new();
        marshall_string(&mut w, Some("hello"));
        marshall_string(&mut w, None);
        marshall_uuid(&mut w, Some(0x0102030405060708090a0b0c0d0e0f10u128), true);
        marshall_uuid(&mut w, None, true);
        marshall_enum_ordinal(&mut w, Some(3));
        marshall_enum_ordinal(&mut w, None);
        let bytes = w.into_bytes();

        let mut r = ByteReader::new(&bytes);
        assert_eq!(unmarshall_string(&mut r).unwrap().as_deref(), Some("hello"));
        assert_eq!(unmarshall_string(&mut r).unwrap(), None);
        assert_eq!(
            unmarshall_uuid(&mut r, true).unwrap(),
            Some(0x0102030405060708090a0b0c0d0e0f10u128)
        );
        assert_eq!(unmarshall_uuid(&mut r, true).unwrap(), None);
        assert_eq!(unmarshall_enum_ordinal(&mut r).unwrap(), Some(3));
        assert_eq!(unmarshall_enum_ordinal(&mut r).unwrap(), None);
        assert_eq!(r.remaining(), 0);
    }
}
