//! Port of Infinispan's `BytesObjectOutput` / `BytesObjectInput` / `AbstractBytesObjectInput` /
//! `Primitives` (`core/src/main/java/org/infinispan/marshall/core/`, 14.0.35.Final).
//!
//! This is the raw byte-level writer/reader `GlobalMarshaller` builds on, plus the `PRIMITIVES`
//! type-byte dispatch table (`Primitives.java`) for boxed primitives, `String`, and primitive
//! arrays. Cross-checked byte-for-byte against real vectors produced by the unmodified upstream
//! Java classes — see `tests/vectors/primitives.hex` and `tests/vectors/README.md`.
//!
//! `BytesObjectOutput.writeUTF`/`readUTF` here is Infinispan's own 4-byte-length modified-UTF-8
//! (`BytesObjectOutput.java:156-204`), distinct from the JDK `ObjectOutputStream.writeUTF`
//! 2-byte-length format used by `java_serial` (`TC_STRING`) — the two must not be conflated.

use super::error::{Error, Result};

/// Mirrors `BytesObjectOutput`: an expandable, position-tracked byte buffer with Infinispan's
/// primitive and string encodings (`Primitives.java`, `BytesObjectOutput.java`).
#[derive(Debug, Default, Clone)]
pub struct ByteWriter {
    buf: Vec<u8>,
}

impl ByteWriter {
    /// An empty buffer.
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// An empty buffer with room for `cap` bytes.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap),
        }
    }

    /// The bytes written.
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    /// The bytes written so far.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// The number of bytes written.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether nothing was written.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Writes `bytes` as they are.
    pub fn write_raw(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// `writeBoolean`: `1` or `0`.
    pub fn write_boolean(&mut self, v: bool) {
        self.buf.push(if v { 1 } else { 0 });
    }

    /// `writeByte`.
    pub fn write_byte(&mut self, v: i8) {
        self.buf.push(v as u8);
    }

    /// Writes one byte.
    pub fn write_unsigned_byte(&mut self, v: u8) {
        self.buf.push(v);
    }

    /// `writeShort`, big-endian.
    pub fn write_short(&mut self, v: i16) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// An unsigned 16-bit value, big-endian.
    pub fn write_unsigned_short(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// Java `char` is an unsigned UTF-16 code unit; `writeChar` and `writeShort` share the same
    /// 2-byte big-endian wire layout in `BytesObjectOutput`.
    pub fn write_char(&mut self, v: u16) {
        self.write_unsigned_short(v);
    }

    /// `writeInt`, big-endian.
    pub fn write_int(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// `writeLong`, big-endian.
    pub fn write_long(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// `writeFloat`: the IEEE 754 bits, big-endian.
    pub fn write_float(&mut self, v: f32) {
        self.write_int(v.to_bits() as i32);
    }

    /// `writeDouble`: the IEEE 754 bits, big-endian.
    pub fn write_double(&mut self, v: f64) {
        self.write_long(v.to_bits() as i64);
    }

    /// `BytesObjectOutput.writeString` (`:125-139`): `0` empty, `1 | len:u8 | ascii-bytes` when
    /// non-empty, ASCII-only and shorter than 64 chars, else `2` followed by `writeUTF` (this
    /// crate's `write_utf`, the 4-byte-length modified-UTF-8 form below).
    pub fn write_string(&mut self, s: &str) {
        if s.is_empty() {
            self.write_unsigned_byte(0);
            return;
        }
        let chars: Vec<char> = s.chars().collect();
        let is_ascii = chars.len() < 64 && chars.iter().all(|c| (*c as u32) <= 127);
        if is_ascii {
            self.write_unsigned_byte(1);
            self.write_unsigned_byte(chars.len() as u8);
            for c in chars {
                self.buf.push(c as u8);
            }
        } else {
            self.write_unsigned_byte(2);
            self.write_utf(s);
        }
    }

    /// `BytesObjectOutput.writeUTF` (`:156-204`): modified UTF-8 (encoded per UTF-16 code unit,
    /// like `DataOutput.writeUTF`) but framed with a **4-byte** big-endian byte-length prefix
    /// instead of the JDK's 2-byte prefix, AND with a genuine upstream quirk in how it encodes a
    /// NUL code unit — see `infinispan_write_utf_bytes`, which this defers to (it is *not* the
    /// same encoder as `java_serial`'s or `marshall_util`'s standard modified-UTF-8, which always
    /// use the overlong two-byte NUL form).
    pub fn write_utf(&mut self, s: &str) {
        let encoded = infinispan_write_utf_bytes(s);
        self.write_int(encoded.len() as i32);
        self.buf.extend_from_slice(&encoded);
    }

    /// `Primitives.writePrimitive`: `ID_PRIMITIVE` sub-tag byte, then the raw payload.
    pub fn write_primitive(&mut self, value: &PrimitiveValue) {
        self.write_unsigned_byte(value.id());
        self.write_raw_primitive(value);
    }

    /// `Primitives.writeRawPrimitive`: the payload only, no sub-tag byte (used for typed array
    /// elements, where the sub-tag is written once for the whole array).
    pub fn write_raw_primitive(&mut self, value: &PrimitiveValue) {
        use PrimitiveValue::*;
        match value {
            ByteArray(b) => self.write_length_framed(b.len(), |w| w.write_raw(b)),
            Str(s) => self.write_string(s),
            BooleanObj(b) => self.write_boolean(*b),
            ByteObj(b) => self.write_byte(*b),
            CharObj(c) => self.write_char(*c),
            DoubleObj(d) => self.write_double(*d),
            FloatObj(f) => self.write_float(*f),
            IntObj(i) => self.write_int(*i),
            LongObj(l) => self.write_long(*l),
            ShortObj(s) => self.write_short(*s),
            BooleanArray(a) => self.write_length_framed(a.len(), |w| w.write_bools(a)),
            CharArray(a) => self.write_length_framed(a.len(), |w| {
                for c in a {
                    w.write_char(*c);
                }
            }),
            DoubleArray(a) => self.write_length_framed(a.len(), |w| {
                for d in a {
                    w.write_double(*d);
                }
            }),
            FloatArray(a) => self.write_length_framed(a.len(), |w| {
                for f in a {
                    w.write_float(*f);
                }
            }),
            IntArray(a) => self.write_length_framed(a.len(), |w| {
                for i in a {
                    w.write_int(*i);
                }
            }),
            LongArray(a) => self.write_length_framed(a.len(), |w| {
                for l in a {
                    w.write_long(*l);
                }
            }),
            ShortArray(a) => self.write_length_framed(a.len(), |w| {
                for s in a {
                    w.write_short(*s);
                }
            }),
        }
    }

    /// The empty/small/medium/large array framing shared by every `Primitives.write*Array`
    /// method: `ID_ARRAY_EMPTY` (no length), `ID_ARRAY_SMALL | len-1:u8` (<=256),
    /// `ID_ARRAY_MEDIUM | (len-257):u16` (<=0x10100), `ID_ARRAY_LARGE | len:u32` otherwise.
    fn write_length_framed(&mut self, len: usize, body: impl FnOnce(&mut Self)) {
        if len == 0 {
            self.write_unsigned_byte(ID_ARRAY_EMPTY);
        } else if len <= SMALL_ARRAY_MAX {
            self.write_unsigned_byte(ID_ARRAY_SMALL);
            self.write_unsigned_byte((len - SMALL_ARRAY_MIN) as u8);
        } else if len <= MEDIUM_ARRAY_MAX {
            self.write_unsigned_byte(ID_ARRAY_MEDIUM);
            self.write_unsigned_short((len - MEDIUM_ARRAY_MIN) as u16);
        } else {
            self.write_unsigned_byte(ID_ARRAY_LARGE);
            self.write_int(len as i32);
        }
        body(self);
    }

    /// `Primitives.writeBooleans`: 8 bits packed per byte, LSB first, tail byte for a remainder.
    fn write_bools(&mut self, bits: &[bool]) {
        let bc = bits.len() - (bits.len() % 8);
        let mut i = 0;
        while i < bc {
            let mut byte = 0u8;
            for shift in 0..8 {
                if bits[i + shift] {
                    byte |= 1 << shift;
                }
            }
            self.buf.push(byte);
            i += 8;
        }
        if bc < bits.len() {
            let mut byte = 0u8;
            for (shift, b) in bits[bc..].iter().enumerate() {
                if *b {
                    byte |= 1 << shift;
                }
            }
            self.write_unsigned_byte(byte);
        }
    }
}

/// Mirrors `AbstractBytesObjectInput` / `BytesObjectInput`: a position-tracked reader over a byte
/// slice with the matching decode side of every `ByteWriter` method.
#[derive(Debug, Clone, Copy)]
pub struct ByteReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> ByteReader<'a> {
    /// A reader at the start of `buf`.
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// The offset of the next byte to read.
    pub fn pos(&self) -> usize {
        self.pos
    }

    /// How many bytes are left.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn check(&self, len: usize) -> Result<()> {
        if self.pos + len > self.buf.len() {
            Err(Error::Eof)
        } else {
            Ok(())
        }
    }

    /// Borrows the next `len` bytes.
    pub fn read_raw(&mut self, len: usize) -> Result<&'a [u8]> {
        self.check(len)?;
        let slice = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(slice)
    }

    /// `readBoolean`: any non-zero byte is `true`.
    pub fn read_boolean(&mut self) -> Result<bool> {
        Ok(self.read_byte()? != 0)
    }

    /// `readByte`.
    pub fn read_byte(&mut self) -> Result<i8> {
        self.check(1)?;
        let v = self.buf[self.pos] as i8;
        self.pos += 1;
        Ok(v)
    }

    /// `readUnsignedByte`.
    pub fn read_unsigned_byte(&mut self) -> Result<u8> {
        Ok(self.read_byte()? as u8)
    }

    /// `readShort`, big-endian.
    pub fn read_short(&mut self) -> Result<i16> {
        let b = self.read_raw(2)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    /// `readUnsignedShort`, big-endian.
    pub fn read_unsigned_short(&mut self) -> Result<u16> {
        let b = self.read_raw(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    /// `readChar`: one UTF-16 code unit.
    pub fn read_char(&mut self) -> Result<u16> {
        self.read_unsigned_short()
    }

    /// `readInt`, big-endian.
    pub fn read_int(&mut self) -> Result<i32> {
        let b = self.read_raw(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// `readLong`, big-endian.
    pub fn read_long(&mut self) -> Result<i64> {
        let b = self.read_raw(8)?;
        let mut arr = [0u8; 8];
        arr.copy_from_slice(b);
        Ok(i64::from_be_bytes(arr))
    }

    /// `readFloat`.
    pub fn read_float(&mut self) -> Result<f32> {
        Ok(f32::from_bits(self.read_int()? as u32))
    }

    /// `readDouble`.
    pub fn read_double(&mut self) -> Result<f64> {
        Ok(f64::from_bits(self.read_long()? as u64))
    }

    /// `AbstractBytesObjectInput.readString` (`:239-257`).
    pub fn read_string(&mut self) -> Result<String> {
        match self.read_unsigned_byte()? {
            0 => Ok(String::new()),
            1 => {
                let size = self.read_unsigned_byte()? as usize;
                let bytes = self.read_raw(size)?;
                // Upstream treats these as Latin-1/ASCII bytes (the deprecated
                // `String(byte[], int, int, int)` ctor with hibyte=0), matching the ASCII-only
                // guarantee `writeString` enforces before choosing this branch.
                Ok(bytes.iter().map(|&b| b as char).collect())
            }
            2 => self.read_utf(),
            other => Err(Error::UnknownTag {
                what: "string marker",
                value: other as i64,
            }),
        }
    }

    /// `AbstractBytesObjectInput.readUTF` (`:172-237`): 4-byte length, then modified UTF-8.
    pub fn read_utf(&mut self) -> Result<String> {
        let len = self.read_int()? as usize;
        let bytes = self.read_raw(len)?;
        modified_utf8_decode(bytes)
    }

    /// `Primitives.readPrimitive`: a type byte, then the value.
    pub fn read_primitive(&mut self) -> Result<PrimitiveValue> {
        let id = self.read_unsigned_byte()?;
        self.read_raw_primitive(id)
    }

    /// The value of type byte `id`, already read.
    pub fn read_raw_primitive(&mut self, id: u8) -> Result<PrimitiveValue> {
        use PrimitiveValue::*;
        Ok(match id {
            ID_BYTE_ARRAY => ByteArray(self.read_length_framed(|r, n| Ok(r.read_raw(n)?.to_vec()))?),
            ID_STRING => Str(self.read_string()?),
            ID_BOOLEAN_OBJ => BooleanObj(self.read_boolean()?),
            ID_BYTE_OBJ => ByteObj(self.read_byte()?),
            ID_CHAR_OBJ => CharObj(self.read_char()?),
            ID_DOUBLE_OBJ => DoubleObj(self.read_double()?),
            ID_FLOAT_OBJ => FloatObj(self.read_float()?),
            ID_INT_OBJ => IntObj(self.read_int()?),
            ID_LONG_OBJ => LongObj(self.read_long()?),
            ID_SHORT_OBJ => ShortObj(self.read_short()?),
            ID_BOOLEAN_ARRAY => BooleanArray(self.read_length_framed(|r, n| r.read_bools(n))?),
            ID_CHAR_ARRAY => CharArray(self.read_length_framed(|r, n| {
                (0..n).map(|_| r.read_char()).collect()
            })?),
            ID_DOUBLE_ARRAY => DoubleArray(self.read_length_framed(|r, n| {
                (0..n).map(|_| r.read_double()).collect()
            })?),
            ID_FLOAT_ARRAY => FloatArray(self.read_length_framed(|r, n| {
                (0..n).map(|_| r.read_float()).collect()
            })?),
            ID_INT_ARRAY => IntArray(self.read_length_framed(|r, n| {
                (0..n).map(|_| r.read_int()).collect()
            })?),
            ID_LONG_ARRAY => LongArray(self.read_length_framed(|r, n| {
                (0..n).map(|_| r.read_long()).collect()
            })?),
            ID_SHORT_ARRAY => ShortArray(self.read_length_framed(|r, n| {
                (0..n).map(|_| r.read_short()).collect()
            })?),
            other => {
                return Err(Error::UnknownTag {
                    what: "primitive sub-id",
                    value: other as i64,
                });
            }
        })
    }

    fn read_length_framed<T>(
        &mut self,
        body: impl FnOnce(&mut Self, usize) -> Result<T>,
    ) -> Result<T> {
        let tag = self.read_unsigned_byte()?;
        let len = match tag {
            ID_ARRAY_EMPTY => 0,
            ID_ARRAY_SMALL => self.read_unsigned_byte()? as usize + SMALL_ARRAY_MIN,
            ID_ARRAY_MEDIUM => self.read_unsigned_short()? as usize + MEDIUM_ARRAY_MIN,
            ID_ARRAY_LARGE => self.read_int()? as usize,
            other => {
                return Err(Error::UnknownTag {
                    what: "array size tag",
                    value: other as i64,
                });
            }
        };
        body(self, len)
    }

    fn read_bools(&mut self, len: usize) -> Result<Vec<bool>> {
        // one bit per element: refuse a length the buffer cannot hold before allocating for it
        self.check(len.div_ceil(8))?;
        let mut out = vec![false; len];
        let bc = len - (len % 8);
        let mut i = 0;
        while i < bc {
            let v = self.read_unsigned_byte()?;
            for shift in 0..8 {
                out[i + shift] = (v & (1 << shift)) != 0;
            }
            i += 8;
        }
        if bc < len {
            let v = self.read_unsigned_byte()?;
            for (shift, slot) in out[bc..].iter_mut().enumerate() {
                *slot = (v & (1 << shift)) != 0;
            }
        }
        Ok(out)
    }
}

// Primitives.java type-byte constants.
/// `Primitives.ID_BYTE_ARRAY`: the type byte of a `byte[]`.
pub const ID_BYTE_ARRAY: u8 = 0x01;
/// `Primitives.ID_STRING`: the type byte of a `String`.
pub const ID_STRING: u8 = 0x02;
/// `Primitives.ID_BOOLEAN_OBJ`: the type byte of a `Boolean`.
pub const ID_BOOLEAN_OBJ: u8 = 0x10;
/// `Primitives.ID_BYTE_OBJ`: the type byte of a `Byte`.
pub const ID_BYTE_OBJ: u8 = 0x11;
/// `Primitives.ID_CHAR_OBJ`: the type byte of a `Character`.
pub const ID_CHAR_OBJ: u8 = 0x12;
/// `Primitives.ID_DOUBLE_OBJ`: the type byte of a `Double`.
pub const ID_DOUBLE_OBJ: u8 = 0x13;
/// `Primitives.ID_FLOAT_OBJ`: the type byte of a `Float`.
pub const ID_FLOAT_OBJ: u8 = 0x14;
/// `Primitives.ID_INT_OBJ`: the type byte of a `Integer`.
pub const ID_INT_OBJ: u8 = 0x15;
/// `Primitives.ID_LONG_OBJ`: the type byte of a `Long`.
pub const ID_LONG_OBJ: u8 = 0x16;
/// `Primitives.ID_SHORT_OBJ`: the type byte of a `Short`.
pub const ID_SHORT_OBJ: u8 = 0x17;
/// `Primitives.ID_BOOLEAN_ARRAY`: the type byte of a `boolean[]`.
pub const ID_BOOLEAN_ARRAY: u8 = 0x18;
/// `Primitives.ID_CHAR_ARRAY`: the type byte of a `char[]`.
pub const ID_CHAR_ARRAY: u8 = 0x19;
/// `Primitives.ID_DOUBLE_ARRAY`: the type byte of a `double[]`.
pub const ID_DOUBLE_ARRAY: u8 = 0x1A;
/// `Primitives.ID_FLOAT_ARRAY`: the type byte of a `float[]`.
pub const ID_FLOAT_ARRAY: u8 = 0x1B;
/// `Primitives.ID_INT_ARRAY`: the type byte of a `int[]`.
pub const ID_INT_ARRAY: u8 = 0x1C;
/// `Primitives.ID_LONG_ARRAY`: the type byte of a `long[]`.
pub const ID_LONG_ARRAY: u8 = 0x1D;
/// `Primitives.ID_SHORT_ARRAY`: the type byte of a `short[]`.
pub const ID_SHORT_ARRAY: u8 = 0x1E;

const ID_ARRAY_EMPTY: u8 = 0x28;
const ID_ARRAY_SMALL: u8 = 0x29;
const ID_ARRAY_MEDIUM: u8 = 0x2A;
const ID_ARRAY_LARGE: u8 = 0x2B;

const SMALL_ARRAY_MIN: usize = 0x1;
const SMALL_ARRAY_MAX: usize = 0x100;
const MEDIUM_ARRAY_MIN: usize = 0x101;
const MEDIUM_ARRAY_MAX: usize = 0x10100;

/// One `Primitives.PRIMITIVES`-dispatchable value.
#[derive(Debug, Clone, PartialEq)]
pub enum PrimitiveValue {
    /// A `byte[]`.
    ByteArray(Vec<u8>),
    /// A `String`.
    Str(String),
    /// A `Boolean`.
    BooleanObj(bool),
    /// A `Byte`.
    ByteObj(i8),
    /// A `Character`.
    CharObj(u16),
    /// A `Double`.
    DoubleObj(f64),
    /// A `Float`.
    FloatObj(f32),
    /// A `Integer`.
    IntObj(i32),
    /// A `Long`.
    LongObj(i64),
    /// A `Short`.
    ShortObj(i16),
    /// A `boolean[]`.
    BooleanArray(Vec<bool>),
    /// A `char[]`.
    CharArray(Vec<u16>),
    /// A `double[]`.
    DoubleArray(Vec<f64>),
    /// A `float[]`.
    FloatArray(Vec<f32>),
    /// A `int[]`.
    IntArray(Vec<i32>),
    /// A `long[]`.
    LongArray(Vec<i64>),
    /// A `short[]`.
    ShortArray(Vec<i16>),
}

impl PrimitiveValue {
    /// The value's type byte, one of the `ID_*` constants.
    pub fn id(&self) -> u8 {
        use PrimitiveValue::*;
        match self {
            ByteArray(_) => ID_BYTE_ARRAY,
            Str(_) => ID_STRING,
            BooleanObj(_) => ID_BOOLEAN_OBJ,
            ByteObj(_) => ID_BYTE_OBJ,
            CharObj(_) => ID_CHAR_OBJ,
            DoubleObj(_) => ID_DOUBLE_OBJ,
            FloatObj(_) => ID_FLOAT_OBJ,
            IntObj(_) => ID_INT_OBJ,
            LongObj(_) => ID_LONG_OBJ,
            ShortObj(_) => ID_SHORT_OBJ,
            BooleanArray(_) => ID_BOOLEAN_ARRAY,
            CharArray(_) => ID_CHAR_ARRAY,
            DoubleArray(_) => ID_DOUBLE_ARRAY,
            FloatArray(_) => ID_FLOAT_ARRAY,
            IntArray(_) => ID_INT_ARRAY,
            LongArray(_) => ID_LONG_ARRAY,
            ShortArray(_) => ID_SHORT_ARRAY,
        }
    }
}

/// Java's "modified UTF-8" (as used by both `DataOutput.writeUTF` and, with a different length
/// prefix, `BytesObjectOutput.writeUTF`): like UTF-8 but the NUL code point is encoded as the
/// two-byte overlong form `C0 80` rather than one zero byte, and encoding is per UTF-16 code
/// unit (so an astral character comes out as a CESU-8-style surrogate pair, not a 4-byte
/// sequence) — matching `BytesObjectOutput.writeUTF`'s `char`-at-a-time loop exactly.
pub(crate) fn modified_utf8_encode(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    for unit in s.encode_utf16() {
        match unit {
            0x0001..=0x007F => out.push(unit as u8),
            0..=0x07FF => {
                out.push(0xC0 | ((unit >> 6) as u8 & 0x1F));
                out.push(0x80 | (unit as u8 & 0x3F));
            }
            _ => {
                out.push(0xE0 | ((unit >> 12) as u8 & 0x0F));
                out.push(0x80 | ((unit >> 6) as u8 & 0x3F));
                out.push(0x80 | (unit as u8 & 0x3F));
            }
        }
    }
    out
}

/// `BytesObjectOutput.writeUTF`'s own encoder (`BytesObjectOutput.java:156-204`), which is
/// **not** standard modified UTF-8: its first loop copies any code unit `<= 127` (a NUL code
/// unit included) as a single raw byte, but if it ever breaks out into the second loop (on
/// hitting the first code unit `> 127`), that second loop's single-byte range is `1..=0x7F` —
/// **excluding** NUL, which then falls through to the two-byte overlong form `C0 80` like normal
/// modified UTF-8. So a leading NUL is encoded as one `0x00` byte, but a NUL anywhere after the
/// first non-ASCII character is encoded as `C0 80` — a real upstream inconsistency, confirmed
/// against `tests/vectors/primitives.hex`'s `writeString_multibyte` vector (`"\u0000߿ࠀ
/// ￿"`, whose first 3 bytes are `00 df bf`, not `c0 80 df bf`), not assumed from reading the
/// source alone. The decode side is unaffected: `modified_utf8_decode`'s first loop already
/// accepts a raw `0x00` byte as code unit 0 (its condition is only `c > 127`), so both of this
/// encoder's forms for NUL decode back correctly — no matching "quirky" decoder is needed.
pub(crate) fn infinispan_write_utf_bytes(s: &str) -> Vec<u8> {
    let units: Vec<u16> = s.encode_utf16().collect();
    let mut out = Vec::with_capacity(units.len());
    let mut i = 0;
    while i < units.len() {
        let c = units[i];
        if c > 127 {
            break;
        }
        out.push(c as u8);
        i += 1;
    }
    while i < units.len() {
        let c = units[i];
        if (0x0001..=0x007F).contains(&c) {
            out.push(c as u8);
        } else if c > 0x07FF {
            out.push(0xE0 | ((c >> 12) as u8 & 0x0F));
            out.push(0x80 | ((c >> 6) as u8 & 0x3F));
            out.push(0x80 | (c as u8 & 0x3F));
        } else {
            out.push(0xC0 | ((c >> 6) as u8 & 0x1F));
            out.push(0x80 | (c as u8 & 0x3F));
        }
        i += 1;
    }
    out
}

pub(crate) fn modified_utf8_decode(bytes: &[u8]) -> Result<String> {
    let mut units: Vec<u16> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as u32;
        if c >> 7 == 0 {
            units.push(c as u16);
            i += 1;
        } else if c >> 5 == 0b110 {
            if i + 1 >= bytes.len() {
                return Err(Error::Malformed("partial character at end".into()));
            }
            let c2 = bytes[i + 1] as u32;
            if c2 & 0xC0 != 0x80 {
                return Err(Error::Malformed(format!("malformed input around byte {i}")));
            }
            units.push((((c & 0x1F) << 6) | (c2 & 0x3F)) as u16);
            i += 2;
        } else if c >> 4 == 0b1110 {
            if i + 2 >= bytes.len() {
                return Err(Error::Malformed("partial character at end".into()));
            }
            let c2 = bytes[i + 1] as u32;
            let c3 = bytes[i + 2] as u32;
            if c2 & 0xC0 != 0x80 || c3 & 0xC0 != 0x80 {
                return Err(Error::Malformed(format!("malformed input around byte {i}")));
            }
            units.push((((c & 0x0F) << 12) | ((c2 & 0x3F) << 6) | (c3 & 0x3F)) as u16);
            i += 3;
        } else {
            return Err(Error::Malformed(format!("malformed input around byte {i}")));
        }
    }
    String::from_utf16(&units).map_err(|e| Error::Malformed(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `label\thex` lines from a real-Java-produced fixture file.
    fn load_vectors(path: &str) -> Vec<(String, Vec<u8>)> {
        let text = std::fs::read_to_string(path).expect("fixture file");
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|line| {
                let (label, hex) = line.split_once('\t').expect("label\\thex");
                (label.to_string(), hex_decode(hex))
            })
            .collect()
    }

    fn hex_decode(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn vector(vectors: &[(String, Vec<u8>)], label: &str) -> Vec<u8> {
        vectors
            .iter()
            .find(|(l, _)| l == label)
            .unwrap_or_else(|| panic!("no vector named {label}"))
            .1
            .clone()
    }

    #[test]
    fn raw_writes_match_real_java_vectors() {
        let vectors = load_vectors(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/vectors/primitives.hex"));

        let mut w = ByteWriter::new();
        w.write_boolean(true);
        assert_eq!(w.into_bytes(), vector(&vectors, "writeBoolean_true"));

        let mut w = ByteWriter::new();
        w.write_boolean(false);
        assert_eq!(w.into_bytes(), vector(&vectors, "writeBoolean_false"));

        let cases_byte: &[(&str, i8)] = &[
            ("writeByte_0", 0),
            ("writeByte_127", 127),
            ("writeByte_-128", -128),
            ("writeByte_255", -1), // Java writeByte(255) truncates to (byte) 255 == -1
        ];
        for (label, v) in cases_byte {
            let mut w = ByteWriter::new();
            w.write_byte(*v);
            assert_eq!(w.into_bytes(), vector(&vectors, label), "{label}");
        }

        let cases_short: &[(&str, i16)] = &[
            ("writeShort_0", 0),
            ("writeShort_32767", 32767),
            ("writeShort_-32768", -32768),
        ];
        for (label, v) in cases_short {
            let mut w = ByteWriter::new();
            w.write_short(*v);
            assert_eq!(w.into_bytes(), vector(&vectors, label), "{label}");
        }

        let mut w = ByteWriter::new();
        w.write_char('A' as u16);
        assert_eq!(w.into_bytes(), vector(&vectors, "writeChar_A"));

        let mut w = ByteWriter::new();
        w.write_char(0x1234);
        assert_eq!(w.into_bytes(), vector(&vectors, "writeChar_unicode"));

        let cases_int: &[(&str, i32)] = &[
            ("writeInt_0", 0),
            ("writeInt_1", 1),
            ("writeInt_-1", -1),
            ("writeInt_max", i32::MAX),
            ("writeInt_min", i32::MIN),
        ];
        for (label, v) in cases_int {
            let mut w = ByteWriter::new();
            w.write_int(*v);
            assert_eq!(w.into_bytes(), vector(&vectors, label), "{label}");
        }

        let cases_long: &[(&str, i64)] = &[
            ("writeLong_0", 0),
            ("writeLong_max", i64::MAX),
            ("writeLong_min", i64::MIN),
        ];
        for (label, v) in cases_long {
            let mut w = ByteWriter::new();
            w.write_long(*v);
            assert_eq!(w.into_bytes(), vector(&vectors, label), "{label}");
        }

        let mut w = ByteWriter::new();
        w.write_float(1.5);
        assert_eq!(w.into_bytes(), vector(&vectors, "writeFloat_1.5"));
        let mut w = ByteWriter::new();
        w.write_float(-0.0);
        assert_eq!(w.into_bytes(), vector(&vectors, "writeFloat_-0.0"));

        let mut w = ByteWriter::new();
        w.write_double(1.5);
        assert_eq!(w.into_bytes(), vector(&vectors, "writeDouble_1.5"));
        let mut w = ByteWriter::new();
        w.write_double(f64::NAN);
        assert_eq!(w.into_bytes(), vector(&vectors, "writeDouble_nan"));
    }

    #[test]
    fn string_encoding_matches_real_java_vectors() {
        let vectors = load_vectors(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/vectors/primitives.hex"));

        let cases: &[(&str, &str)] = &[
            ("writeString_empty", ""),
            ("writeString_ascii_short", "hello"),
            ("writeString_non_ascii", "héllo\u{e9}"),
            ("writeString_multibyte", "\u{0}\u{7ff}\u{800}\u{ffff}"),
        ];
        for (label, s) in cases {
            let mut w = ByteWriter::new();
            w.write_string(s);
            assert_eq!(w.into_bytes(), vector(&vectors, label), "{label}");
        }

        let ascii_63 = "x".repeat(63);
        let mut w = ByteWriter::new();
        w.write_string(&ascii_63);
        assert_eq!(w.into_bytes(), vector(&vectors, "writeString_ascii_63"));

        let ascii_64 = "x".repeat(64);
        let mut w = ByteWriter::new();
        w.write_string(&ascii_64);
        assert_eq!(w.into_bytes(), vector(&vectors, "writeString_ascii_64_forces_utf"));

        let utf_cases: &[(&str, &str)] = &[
            ("writeUTF_empty", ""),
            ("writeUTF_ascii", "hello world"),
            ("writeUTF_non_ascii", "héllo\u{e9} \u{4e2d}\u{6587}"),
            ("writeUTF_null_char", "a\u{0}b"),
        ];
        for (label, s) in utf_cases {
            let mut w = ByteWriter::new();
            w.write_utf(s);
            assert_eq!(w.into_bytes(), vector(&vectors, label), "{label}");
        }
    }

    #[test]
    #[allow(clippy::approx_constant)] // 3.14159 is the literal value the Java vector generator used
    fn primitive_dispatch_matches_real_java_vectors() {
        let vectors = load_vectors(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/vectors/primitives.hex"));

        let cases: &[(&str, PrimitiveValue)] = &[
            ("Boolean_true", PrimitiveValue::BooleanObj(true)),
            ("Boolean_false", PrimitiveValue::BooleanObj(false)),
            ("Byte_42", PrimitiveValue::ByteObj(42)),
            ("Character_Z", PrimitiveValue::CharObj('Z' as u16)),
            ("Double_pi", PrimitiveValue::DoubleObj(3.14159)),
            ("Float_pi", PrimitiveValue::FloatObj(3.14159)),
            ("Integer_12345", PrimitiveValue::IntObj(12345)),
            ("Integer_negative", PrimitiveValue::IntObj(-98765)),
            ("Long_big", PrimitiveValue::LongObj(123456789012345)),
            ("Short_300", PrimitiveValue::ShortObj(300)),
            (
                "String_hello",
                PrimitiveValue::Str("hello, primitives".into()),
            ),
            ("byteArray_empty", PrimitiveValue::ByteArray(vec![])),
            (
                "byteArray_small3",
                PrimitiveValue::ByteArray(vec![1, 2, 3]),
            ),
            (
                "byteArray_small256",
                PrimitiveValue::ByteArray((0..256u32).map(|i| (i % 251) as u8).collect()),
            ),
            (
                "byteArray_medium257",
                PrimitiveValue::ByteArray((0..257u32).map(|i| (i % 251) as u8).collect()),
            ),
            ("booleanArray_empty", PrimitiveValue::BooleanArray(vec![])),
            (
                "booleanArray_9",
                PrimitiveValue::BooleanArray(vec![
                    true, false, true, true, false, false, true, false, true,
                ]),
            ),
            (
                "charArray_3",
                PrimitiveValue::CharArray(vec!['a' as u16, 'b' as u16, 'c' as u16]),
            ),
            (
                "doubleArray_2",
                PrimitiveValue::DoubleArray(vec![1.5, -2.5]),
            ),
            ("floatArray_2", PrimitiveValue::FloatArray(vec![1.5, -2.5])),
            ("intArray_3", PrimitiveValue::IntArray(vec![1, -2, 3])),
            ("longArray_2", PrimitiveValue::LongArray(vec![1, -2])),
            ("shortArray_2", PrimitiveValue::ShortArray(vec![1, -2])),
        ];

        for (label, value) in cases {
            let mut w = ByteWriter::new();
            w.write_primitive(value);
            assert_eq!(w.into_bytes(), vector(&vectors, label), "{label}");

            // Round trip through the reader too.
            let bytes = vector(&vectors, label);
            let mut r = ByteReader::new(&bytes);
            let decoded = r.read_primitive().unwrap();
            assert_eq!(&decoded, value, "{label} decode");
            assert_eq!(r.remaining(), 0, "{label} fully consumed");
        }
    }

    #[test]
    fn round_trip_property_all_scalar_writers() {
        // Belt-and-braces: every scalar writer round-trips through its reader, independent of
        // the fixture file.
        let mut w = ByteWriter::new();
        w.write_boolean(true);
        w.write_byte(-5);
        w.write_short(-1234);
        w.write_char(0xBEEF);
        w.write_int(-123456);
        w.write_long(-123456789012345);
        w.write_float(-1.25);
        w.write_double(-1.25e10);
        w.write_string("round\u{1f600}trip"); // outside BMP: exercises the UTF branch
        w.write_utf("modified\u{0}utf8\u{ffff}");
        let bytes = w.into_bytes();

        let mut r = ByteReader::new(&bytes);
        assert!(r.read_boolean().unwrap());
        assert_eq!(r.read_byte().unwrap(), -5);
        assert_eq!(r.read_short().unwrap(), -1234);
        assert_eq!(r.read_char().unwrap(), 0xBEEF);
        assert_eq!(r.read_int().unwrap(), -123456);
        assert_eq!(r.read_long().unwrap(), -123456789012345);
        assert_eq!(r.read_float().unwrap(), -1.25);
        assert_eq!(r.read_double().unwrap(), -1.25e10);
        assert_eq!(r.read_string().unwrap(), "round\u{1f600}trip");
        assert_eq!(r.read_utf().unwrap(), "modified\u{0}utf8\u{ffff}");
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn eof_is_reported_not_panicked() {
        let mut r = ByteReader::new(&[0x01]);
        assert_eq!(r.read_int(), Err(Error::Eof));
    }
}
