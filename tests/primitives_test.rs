//! Port of `marshall/core/PrimitivesTest` (infinispan-core 14.0.35.Final).
//!
//! `testReadAndWrite`: every `Primitives` type round-trips through `writePrimitive`/`readPrimitive`.
//! Java-only part: the `IOException("Unknown primitive type: diable")` for a value that is not a
//! primitive cannot be written here because `PrimitiveValue` is closed; its counterpart is reading
//! a primitive sub-id nobody defined. `testLargeArray` expects an `OutOfMemoryError` from
//! `BytesObjectOutput.write(bytes, 0, Integer.MAX_VALUE)`; the Rust counterpart is the read side:
//! an array length the buffer cannot hold is an error, never an allocation.

use infinispan::marshall::primitives::{ByteReader, ByteWriter, PrimitiveValue};
use infinispan::marshall::Error;

fn assert_read_and_write(value: PrimitiveValue) {
    let mut out = ByteWriter::with_capacity(10240);
    out.write_primitive(&value);
    let bytes = out.into_bytes();
    let mut input = ByteReader::new(&bytes);
    let read = input.read_primitive().unwrap();
    assert_eq!(read, value);
    assert_eq!(input.remaining(), 0, "{value:?} left bytes behind");
}

#[test]
fn read_and_write_every_primitive_type() {
    use PrimitiveValue::*;
    assert_read_and_write(ByteArray(vec![0, 1]));
    assert_read_and_write(Str("kaixo".into()));
    assert_read_and_write(BooleanObj(true));
    assert_read_and_write(ByteObj(0));
    assert_read_and_write(CharObj('P' as u16));
    assert_read_and_write(DoubleObj(123.0));
    assert_read_and_write(FloatObj(123.0));
    assert_read_and_write(IntObj(123));
    assert_read_and_write(LongObj(123));
    assert_read_and_write(ShortObj(123));
    assert_read_and_write(BooleanArray(vec![true, false]));
    assert_read_and_write(CharArray("kaixo".encode_utf16().collect()));
    assert_read_and_write(DoubleArray(vec![123.0, 456.0]));
    assert_read_and_write(FloatArray(vec![123.0, 456.0]));
    assert_read_and_write(IntArray(vec![123, 456]));
    assert_read_and_write(LongArray(vec![123, 456]));
    assert_read_and_write(ShortArray(vec![123, 456]));
}

#[test]
fn an_unknown_primitive_sub_id_is_refused() {
    let bytes = [0x7fu8];
    let err = ByteReader::new(&bytes).read_primitive().unwrap_err();
    assert!(matches!(err, Error::UnknownTag { what: "primitive sub-id", .. }), "{err:?}");
}

#[test]
fn an_array_length_the_buffer_cannot_hold_is_an_error_not_an_allocation() {
    use infinispan::marshall::primitives::{
        ID_BOOLEAN_ARRAY, ID_BYTE_ARRAY, ID_CHAR_ARRAY, ID_DOUBLE_ARRAY, ID_FLOAT_ARRAY, ID_INT_ARRAY, ID_LONG_ARRAY, ID_SHORT_ARRAY,
    };
    const ID_ARRAY_LARGE: u8 = 0x2B; // Primitives.ID_ARRAY_LARGE: a 4-byte length follows
    for id in [ID_BYTE_ARRAY, ID_BOOLEAN_ARRAY, ID_CHAR_ARRAY, ID_DOUBLE_ARRAY, ID_FLOAT_ARRAY, ID_INT_ARRAY, ID_LONG_ARRAY, ID_SHORT_ARRAY] {
        let mut bytes = vec![id, ID_ARRAY_LARGE];
        bytes.extend_from_slice(&i32::MAX.to_be_bytes());
        let err = ByteReader::new(&bytes).read_primitive().unwrap_err();
        assert_eq!(err, Error::Eof, "primitive id {id:#x}");
    }
}
