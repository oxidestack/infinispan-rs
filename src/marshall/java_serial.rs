//! A port of the JDK Object Serialization Stream Protocol — what
//! `org.infinispan.commons.marshall.JavaSerializationMarshaller` uses under the hood
//! (`commons/all/.../marshall/JavaSerializationMarshaller.java`, a thin wrapper around
//! `java.io.ObjectOutputStream`/`ObjectInputStream`). This is Infinispan's "user value" codec:
//! the innermost layer of the `GlobalMarshaller` `ID_UNKNOWN` path
//! (see [`super::wrapped_message`]).
//!
//! Byte-exactness is proven against real, `java`-produced vectors — see
//! `tests/vectors/java_serial.hex` and `tests/vectors/README.md` — via **decode-then-re-encode**
//! round trips: `encode_stream(&decode_stream(real_bytes)?) == real_bytes`. That is a stronger
//! test than hand-constructing a matching [`JavaValue`] tree and comparing to the fixture: it
//! doesn't require this port to already know real classes' `serialVersionUID` constants (the
//! decoder recovers them from the stream itself), and it exercises both directions against real
//! upstream output.
//!
//! Protocol reference: the informal grammar below reflects what was independently confirmed by
//! hand-decoding `tests/vectors/java_serial.hex` byte for byte against known field names,
//! `serialVersionUID`s and class names (see the vector generator's inputs) — not transcribed
//! from memory of the spec document.
//!
//! ```text
//! stream:          magic(u16=0xaced) version(u16=5) content*
//! content:         object | TC_BLOCKDATA | TC_BLOCKDATALONG   (only `object` used at top level here)
//! object:          TC_NULL | TC_REFERENCE handle | TC_STRING (utf16len bytes) | TC_LONGSTRING (utf64len bytes)
//!                 | TC_ARRAY classDesc newHandle size element*   (only primitive `byte` component ported)
//!                 | TC_OBJECT classDesc newHandle classData
//! classDesc:       TC_NULL | TC_REFERENCE handle
//!                 | TC_CLASSDESC className(utf) serialVersionUID(i64) newHandle
//!                   classDescFlags(u8) fieldCount(u16) fieldDesc* classAnnotation(=TC_ENDBLOCKDATA, always empty here)
//!                   superClassDesc(=classDesc, recursive)
//! fieldDesc:       primTypeCode(u8: B C D F I J S Z) name(utf)
//!                 | objTypeCode(u8: L or [) name(utf) className1(utf, RAW — no TC_STRING tag, no handle)
//! classData:       for each class in the chain, ROOT (most-super) first: fieldValue* [annotation if
//!                   SC_WRITE_METHOD or (SC_EXTERNALIZABLE & SC_BLOCK_DATA)]
//! annotation:      (TC_BLOCKDATA len(u8) bytes | TC_BLOCKDATALONG len(u32) bytes)* TC_ENDBLOCKDATA
//! ```

use std::collections::HashMap;
use std::rc::Rc;

use super::allow_list::ClassAllowList;
use super::error::{Error, Result};
use super::primitives::{modified_utf8_decode, modified_utf8_encode, ByteReader, ByteWriter};

/// `ObjectStreamConstants.STREAM_MAGIC`: the first two bytes of a Java serialization stream.
pub const STREAM_MAGIC: u16 = 0xaced;
/// `ObjectStreamConstants.STREAM_VERSION`.
pub const STREAM_VERSION: u16 = 5;

const TC_NULL: u8 = 0x70;
const TC_REFERENCE: u8 = 0x71;
const TC_CLASSDESC: u8 = 0x72;
const TC_OBJECT: u8 = 0x73;
const TC_STRING: u8 = 0x74;
const TC_ARRAY: u8 = 0x75;
const TC_BLOCKDATA: u8 = 0x77;
const TC_ENDBLOCKDATA: u8 = 0x78;
const TC_BLOCKDATALONG: u8 = 0x7A;
const TC_LONGSTRING: u8 = 0x7C;

const BASE_WIRE_HANDLE: u32 = 0x7E0000;

/// `SC_WRITE_METHOD`: the class has a `writeObject` method, so its data is followed by an annotation.
pub const SC_WRITE_METHOD: u8 = 0x01;
/// `SC_SERIALIZABLE`: the class is `Serializable`.
pub const SC_SERIALIZABLE: u8 = 0x02;
/// `SC_EXTERNALIZABLE`: the class is `Externalizable`.
pub const SC_EXTERNALIZABLE: u8 = 0x04;
/// `SC_BLOCK_DATA`: an `Externalizable` class's data is written in block-data mode.
pub const SC_BLOCK_DATA: u8 = 0x08;
/// `SC_ENUM`: the class is an enum.
#[allow(dead_code)] // ported for completeness; no vector needs an enum class descriptor
pub const SC_ENUM: u8 = 0x10;

/// One field descriptor inside a [`ClassDesc`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDesc {
    /// `B C D F I J S Z` for a primitive field, `L` or `[` for an object/array field.
    pub type_code: u8,
    /// The field's name.
    pub name: String,
    /// The field's JVM type signature (e.g. `"Ljava/lang/Object;"`, `"[B"`) — only present for
    /// `L`/`[` fields.
    pub field_class_name: Option<String>,
}

impl FieldDesc {
    /// A field of primitive type `type_code`.
    pub fn primitive(type_code: u8, name: impl Into<String>) -> Self {
        Self {
            type_code,
            name: name.into(),
            field_class_name: None,
        }
    }

    /// An object field with the JVM type signature `jvm_type_signature`.
    pub fn object(name: impl Into<String>, jvm_type_signature: impl Into<String>) -> Self {
        Self {
            type_code: b'L',
            name: name.into(),
            field_class_name: Some(jvm_type_signature.into()),
        }
    }

    fn is_object(&self) -> bool {
        self.type_code == b'L' || self.type_code == b'['
    }
}

/// One `TC_CLASSDESC` — a class name, `serialVersionUID`, flags, its own fields, and (like the
/// real protocol) a link to its superclass's descriptor, if any.
#[derive(Debug, Clone, PartialEq)]
pub struct ClassDesc {
    /// The class's binary name.
    pub name: String,
    /// The class's `serialVersionUID`.
    pub serial_version_uid: i64,
    /// The `SC_*` flags.
    pub flags: u8,
    /// The class's serializable fields, in stream order.
    pub fields: Vec<FieldDesc>,
    /// The superclass's descriptor, if it is serializable.
    pub super_desc: Option<Rc<ClassDesc>>,
}

impl ClassDesc {
    /// The chain from this class up to (but not including) a `None` superclass, in
    /// **root-first** order — matching `classData`'s field-value write order.
    fn chain_root_first(self_rc: &Rc<ClassDesc>) -> Vec<Rc<ClassDesc>> {
        let mut chain = vec![self_rc.clone()];
        let mut cur = self_rc.clone();
        while let Some(sup) = cur.super_desc.clone() {
            chain.push(sup.clone());
            cur = sup;
        }
        chain.reverse();
        chain
    }

    fn has_annotation(&self) -> bool {
        (self.flags & SC_WRITE_METHOD) != 0
            || ((self.flags & SC_EXTERNALIZABLE) != 0 && (self.flags & SC_BLOCK_DATA) != 0)
    }
}

/// One field's runtime value inside an [`ObjectLevel`].
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    /// A `byte` field.
    Byte(i8),
    /// A `char` field.
    Char(u16),
    /// A `double` field.
    Double(f64),
    /// A `float` field.
    Float(f32),
    /// An `int` field.
    Int(i32),
    /// A `long` field.
    Long(i64),
    /// A `short` field.
    Short(i16),
    /// A `boolean` field.
    Bool(bool),
    /// An `L`/`[` field's value — itself a full [`JavaValue`] (`Null`, a nested `Object`, a
    /// `Str`, a `ByteArray`, ...).
    Obj(Box<JavaValue>),
}

/// A chunk of a class's post-fields "annotation" (what a custom `writeObject`/`writeExternal`
/// adds via block-data writes). Nested-object annotation items (`out.writeObject(x)` from inside
/// a custom `writeObject`) are modelled by [`AnnotationItem::Object`] (a
/// `HashMap` writes its entries that way, and `Blob.metadata` can be one).
#[derive(Debug, Clone, PartialEq)]
pub enum AnnotationItem {
    /// Bytes written in block-data mode.
    Block(Vec<u8>),
    /// An object written with `writeObject`.
    Object(JavaValue),
}

/// Per-class-in-the-chain field values and annotation, root class first — see
/// `ClassDesc::chain_root_first`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ObjectLevel {
    /// The values of the class's fields, in descriptor order.
    pub fields: Vec<FieldValue>,
    /// What the class's `writeObject` or `writeExternal` wrote after its fields.
    pub annotation: Vec<AnnotationItem>,
}

/// A decoded (or to-be-encoded) top-level or nested value.
#[derive(Debug, Clone, PartialEq)]
pub enum JavaValue {
    /// `TC_NULL`.
    Null,
    /// `TC_STRING`.
    Str(String),
    /// `TC_ARRAY` with a `byte` component — the only array shape this port needs (`Blob.data`).
    /// Object/other-primitive arrays are not modeled.
    ByteArray {
        /// The array class's descriptor, `[B`.
        class: Rc<ClassDesc>,
        /// The elements.
        bytes: Vec<u8>,
    },
    /// `TC_OBJECT`: an instance of a serializable class.
    Object {
        /// The object's class descriptor.
        class: Rc<ClassDesc>,
        /// One [`ObjectLevel`] per class in `ClassDesc::chain_root_first`, same order.
        levels: Vec<ObjectLevel>,
    },
    /// **Write-only.** A top-level value that must be encoded as `TC_REFERENCE` to an earlier
    /// top-level value in the same `encode_stream` call, modelling real object-identity sharing
    /// (Rust has no implicit equivalent). `usize` indexes the `values` slice passed to
    /// [`encode_stream`]. Never produced by [`decode_stream`] — a decoded stream always resolves
    /// references to their real value.
    SameAs(usize),
}

// ---------------------------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------------------------

struct StreamWriter {
    w: ByteWriter,
    next_handle: u32,
    class_handles: HashMap<String, u32>,
}

impl StreamWriter {
    fn new() -> Self {
        let mut w = ByteWriter::new();
        w.write_unsigned_short(STREAM_MAGIC);
        w.write_unsigned_short(STREAM_VERSION);
        Self {
            w,
            next_handle: 0,
            class_handles: HashMap::new(),
        }
    }

    fn alloc_handle(&mut self) -> u32 {
        let h = BASE_WIRE_HANDLE + self.next_handle;
        self.next_handle += 1;
        h
    }

    fn write_utf(&mut self, s: &str) {
        let bytes = modified_utf8_encode(s);
        self.w.write_unsigned_short(bytes.len() as u16);
        self.w.write_raw(&bytes);
    }

    /// Writes a `classDesc` production (`TC_NULL` / `TC_REFERENCE` / fresh `TC_CLASSDESC`,
    /// deduplicated by class name exactly like a real `ObjectOutputStream` deduplicates by class
    /// identity).
    fn write_class_desc(&mut self, class: Option<&Rc<ClassDesc>>) {
        let class = match class {
            None => {
                self.w.write_unsigned_byte(TC_NULL);
                return;
            }
            Some(c) => c,
        };
        if let Some(&handle) = self.class_handles.get(&class.name) {
            self.w.write_unsigned_byte(TC_REFERENCE);
            self.w.write_int(handle as i32);
            return;
        }

        self.w.write_unsigned_byte(TC_CLASSDESC);
        self.write_utf(&class.name);
        self.w.write_long(class.serial_version_uid);
        let handle = self.alloc_handle();
        self.class_handles.insert(class.name.clone(), handle);

        self.w.write_unsigned_byte(class.flags);
        self.w.write_unsigned_short(class.fields.len() as u16);
        for f in &class.fields {
            self.w.write_unsigned_byte(f.type_code);
            self.write_utf(&f.name);
            if f.is_object() {
                // className1 is written through the normal `object` String path (TC_STRING,
                // consuming a handle) — confirmed empirically against
                // tests/vectors/java_serial.hex's versioned_wrapper_string vector, which has a
                // TC_STRING tag byte (0x74) before this string's length, not a raw UTF pair.
                self.write_value(&JavaValue::Str(
                    f.field_class_name.clone().unwrap_or_default(),
                ));
            }
        }
        // classAnnotation: always empty in this port.
        self.w.write_unsigned_byte(TC_ENDBLOCKDATA);
        self.write_class_desc(class.super_desc.as_ref());
    }

    fn write_annotation(&mut self, items: &[AnnotationItem]) {
        for item in items {
            match item {
                AnnotationItem::Block(bytes) => {
                    if bytes.len() <= u8::MAX as usize {
                        self.w.write_unsigned_byte(TC_BLOCKDATA);
                        self.w.write_unsigned_byte(bytes.len() as u8);
                    } else {
                        self.w.write_unsigned_byte(TC_BLOCKDATALONG);
                        self.w.write_int(bytes.len() as i32);
                    }
                    self.w.write_raw(bytes);
                }
                AnnotationItem::Object(v) => self.write_value(v),
            }
        }
        self.w.write_unsigned_byte(TC_ENDBLOCKDATA);
    }

    fn write_field_value(&mut self, v: &FieldValue) {
        match v {
            FieldValue::Byte(b) => self.w.write_byte(*b),
            FieldValue::Char(c) => self.w.write_char(*c),
            FieldValue::Double(d) => self.w.write_double(*d),
            FieldValue::Float(f) => self.w.write_float(*f),
            FieldValue::Int(i) => self.w.write_int(*i),
            FieldValue::Long(l) => self.w.write_long(*l),
            FieldValue::Short(s) => self.w.write_short(*s),
            FieldValue::Bool(b) => self.w.write_boolean(*b),
            FieldValue::Obj(v) => self.write_value(v),
        }
    }

    /// Writes a full `object` production for a non-top-level (nested) value — never a
    /// `SameAs`, which only makes sense at the top level.
    fn write_value(&mut self, v: &JavaValue) {
        match v {
            JavaValue::Null => {
                self.w.write_unsigned_byte(TC_NULL);
            }
            JavaValue::Str(s) => {
                let bytes = modified_utf8_encode(s);
                if bytes.len() <= u16::MAX as usize {
                    self.w.write_unsigned_byte(TC_STRING);
                    self.w.write_unsigned_short(bytes.len() as u16);
                } else {
                    self.w.write_unsigned_byte(TC_LONGSTRING);
                    self.w.write_long(bytes.len() as i64);
                }
                self.w.write_raw(&bytes);
                self.alloc_handle();
            }
            JavaValue::ByteArray { class, bytes } => {
                self.w.write_unsigned_byte(TC_ARRAY);
                self.write_class_desc(Some(class));
                self.alloc_handle();
                self.w.write_int(bytes.len() as i32);
                self.w.write_raw(bytes);
            }
            JavaValue::Object { class, levels } => {
                self.w.write_unsigned_byte(TC_OBJECT);
                self.write_class_desc(Some(class));
                self.alloc_handle();
                let chain = ClassDesc::chain_root_first(class);
                debug_assert_eq!(
                    chain.len(),
                    levels.len(),
                    "one ObjectLevel per class in the chain"
                );
                for (level, class_at_level) in levels.iter().zip(chain.iter()) {
                    for fv in &level.fields {
                        self.write_field_value(fv);
                    }
                    if class_at_level.has_annotation() {
                        self.write_annotation(&level.annotation);
                    }
                }
            }
            JavaValue::SameAs(_) => panic!("SameAs is only valid as a top-level value"),
        }
    }
}

/// The real, fixed JDK `serialVersionUID` for `byte[]`'s class descriptor
/// (`0xACF317F8060854E0` = `-5984413125824719648L`), confirmed against
/// `tests/vectors/java_serial.hex`'s `blob_default_empty_map` vector's `[B` class descriptor
/// bytes. Only needed when *constructing* a fresh [`JavaValue::ByteArray`] to encode — a
/// decode-then-re-encode round trip instead carries whatever `ClassDesc` was actually read from
/// the stream (see [`JavaValue::ByteArray`]'s field), so it never depends on this constant.
pub fn byte_array_class_desc() -> Rc<ClassDesc> {
    Rc::new(ClassDesc {
        name: "[B".to_string(),
        serial_version_uid: 0xACF317F8060854E0_u64 as i64,
        flags: SC_SERIALIZABLE,
        fields: vec![],
        super_desc: None,
    })
}

/// Encodes a stream containing one or more top-level values (matching a real
/// `ObjectOutputStream` on which `writeObject` was called once per value, in order).
/// [`JavaValue::SameAs`] entries reference an earlier index in `values` by position.
pub fn encode_stream(values: &[JavaValue]) -> Vec<u8> {
    let mut sw = StreamWriter::new();
    let mut top_level_handles: Vec<Option<u32>> = Vec::with_capacity(values.len());
    for v in values {
        match v {
            JavaValue::SameAs(i) => {
                let handle = top_level_handles[*i]
                    .expect("SameAs must reference an earlier value that received a handle");
                sw.w.write_unsigned_byte(TC_REFERENCE);
                sw.w.write_int(handle as i32);
                top_level_handles.push(Some(handle));
            }
            other => {
                let handle_before = sw.next_handle;
                sw.write_value(other);
                let got_handle = sw.next_handle > handle_before;
                top_level_handles.push(if got_handle {
                    Some(BASE_WIRE_HANDLE + handle_before)
                } else {
                    None
                });
            }
        }
    }
    sw.w.into_bytes()
}

/// Encodes `value` as a whole stream, with the stream header.
pub fn encode_one(value: &JavaValue) -> Vec<u8> {
    encode_stream(std::slice::from_ref(value))
}

// ---------------------------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------------------------

#[derive(Clone)]
enum HandleSlot {
    Pending,
    ClassDesc(Rc<ClassDesc>),
    Value(JavaValue),
}

struct StreamReader<'a> {
    r: ByteReader<'a>,
    handles: Vec<HandleSlot>,
    allow_list: Option<&'a ClassAllowList>,
}

impl<'a> StreamReader<'a> {
    fn new(bytes: &'a [u8], allow_list: Option<&'a ClassAllowList>) -> Result<Self> {
        let mut r = ByteReader::new(bytes);
        let magic = r.read_unsigned_short()?;
        let version = r.read_unsigned_short()?;
        if magic != STREAM_MAGIC || version != STREAM_VERSION {
            return Err(Error::Malformed(format!(
                "not a Java serialization stream (magic={magic:#06x} version={version})"
            )));
        }
        Ok(Self {
            r,
            handles: Vec::new(),
            allow_list,
        })
    }

    fn read_utf(&mut self) -> Result<String> {
        let len = self.r.read_unsigned_short()? as usize;
        let bytes = self.r.read_raw(len)?;
        modified_utf8_decode(bytes)
    }

    fn alloc_slot(&mut self) -> usize {
        self.handles.push(HandleSlot::Pending);
        self.handles.len() - 1
    }

    fn handle_index(&self, wire_handle: u32) -> Result<usize> {
        wire_handle
            .checked_sub(BASE_WIRE_HANDLE)
            .map(|i| i as usize)
            .filter(|&i| i < self.handles.len())
            .ok_or_else(|| Error::Malformed(format!("handle {wire_handle:#x} out of range")))
    }

    /// `classDesc`: `TC_NULL` | `TC_REFERENCE` | fresh `TC_CLASSDESC`.
    fn read_class_desc_ref(&mut self) -> Result<Option<Rc<ClassDesc>>> {
        let tag = self.r.read_unsigned_byte()?;
        match tag {
            TC_NULL => Ok(None),
            TC_REFERENCE => {
                let handle = self.r.read_int()? as u32;
                let idx = self.handle_index(handle)?;
                match &self.handles[idx] {
                    HandleSlot::ClassDesc(c) => Ok(Some(c.clone())),
                    _ => Err(Error::Malformed(format!(
                        "handle {handle:#x} does not refer to a class descriptor"
                    ))),
                }
            }
            TC_CLASSDESC => Ok(Some(self.read_fresh_class_desc()?)),
            other => Err(Error::UnknownTag {
                what: "classDesc tag",
                value: other as i64,
            }),
        }
    }

    fn read_fresh_class_desc(&mut self) -> Result<Rc<ClassDesc>> {
        let name = self.read_utf()?;
        let serial_version_uid = self.r.read_long()?;
        let slot = self.alloc_slot();

        if let Some(allow_list) = self.allow_list
            && !allow_list.is_safe_class(&name)
        {
            return Err(Error::NotAllowed(name));
        }

        let flags = self.r.read_unsigned_byte()?;
        let field_count = self.r.read_unsigned_short()?;
        let mut fields = Vec::with_capacity(field_count as usize);
        for _ in 0..field_count {
            let type_code = self.r.read_unsigned_byte()?;
            let fname = self.read_utf()?;
            let field_class_name = if type_code == b'L' || type_code == b'[' {
                // className1 goes through the normal `object` String path — see the matching
                // comment on the writer side.
                match self.read_value()? {
                    JavaValue::Str(s) => Some(s),
                    other => {
                        return Err(Error::Malformed(format!(
                            "field '{fname}' className1 was not a String: {other:?}"
                        )));
                    }
                }
            } else {
                None
            };
            fields.push(FieldDesc {
                type_code,
                name: fname,
                field_class_name,
            });
        }
        // classAnnotation: this port only supports the empty form.
        let ann_tag = self.r.read_unsigned_byte()?;
        if ann_tag != TC_ENDBLOCKDATA {
            return Err(Error::Malformed(format!(
                "class '{name}' has a non-empty class-level annotation (tag {ann_tag:#x}) — not supported by this port"
            )));
        }
        let super_desc = self.read_class_desc_ref()?;

        let class = Rc::new(ClassDesc {
            name,
            serial_version_uid,
            flags,
            fields,
            super_desc,
        });
        self.handles[slot] = HandleSlot::ClassDesc(class.clone());
        Ok(class)
    }

    fn read_annotation(&mut self) -> Result<Vec<AnnotationItem>> {
        let mut items = Vec::new();
        loop {
            let tag = self.r.read_unsigned_byte()?;
            match tag {
                TC_BLOCKDATA => {
                    let len = self.r.read_unsigned_byte()? as usize;
                    items.push(AnnotationItem::Block(self.r.read_raw(len)?.to_vec()));
                }
                TC_BLOCKDATALONG => {
                    let len = self.r.read_int()? as usize;
                    items.push(AnnotationItem::Block(self.r.read_raw(len)?.to_vec()));
                }
                TC_ENDBLOCKDATA => return Ok(items),
                other => items.push(AnnotationItem::Object(self.read_value_with_tag(other)?)),
            }
        }
    }

    fn read_field_value(&mut self, f: &FieldDesc) -> Result<FieldValue> {
        Ok(match f.type_code {
            b'B' => FieldValue::Byte(self.r.read_byte()?),
            b'C' => FieldValue::Char(self.r.read_char()?),
            b'D' => FieldValue::Double(self.r.read_double()?),
            b'F' => FieldValue::Float(self.r.read_float()?),
            b'I' => FieldValue::Int(self.r.read_int()?),
            b'J' => FieldValue::Long(self.r.read_long()?),
            b'S' => FieldValue::Short(self.r.read_short()?),
            b'Z' => FieldValue::Bool(self.r.read_boolean()?),
            b'L' | b'[' => FieldValue::Obj(Box::new(self.read_value()?)),
            other => {
                return Err(Error::UnknownTag {
                    what: "field type code",
                    value: other as i64,
                });
            }
        })
    }

    /// `object`: the full dispatch used both at the top level and for nested `L`/`[` field
    /// values.
    fn read_value(&mut self) -> Result<JavaValue> {
        let tag = self.r.read_unsigned_byte()?;
        self.read_value_with_tag(tag)
    }

    fn read_value_with_tag(&mut self, tag: u8) -> Result<JavaValue> {
        match tag {
            TC_NULL => Ok(JavaValue::Null),
            TC_REFERENCE => {
                let handle = self.r.read_int()? as u32;
                let idx = self.handle_index(handle)?;
                match &self.handles[idx] {
                    HandleSlot::Value(v) => Ok(v.clone()),
                    _ => Err(Error::Malformed(format!(
                        "handle {handle:#x} does not refer to a value"
                    ))),
                }
            }
            TC_STRING => {
                let len = self.r.read_unsigned_short()? as usize;
                let bytes = self.r.read_raw(len)?;
                let s = modified_utf8_decode(bytes)?;
                let slot = self.alloc_slot();
                self.handles[slot] = HandleSlot::Value(JavaValue::Str(s.clone()));
                Ok(JavaValue::Str(s))
            }
            TC_LONGSTRING => {
                let len = self.r.read_long()? as usize;
                let bytes = self.r.read_raw(len)?;
                let s = modified_utf8_decode(bytes)?;
                let slot = self.alloc_slot();
                self.handles[slot] = HandleSlot::Value(JavaValue::Str(s.clone()));
                Ok(JavaValue::Str(s))
            }
            TC_ARRAY => {
                let class = self
                    .read_class_desc_ref()?
                    .ok_or_else(|| Error::Malformed("TC_ARRAY with a null class descriptor".into()))?;
                let slot = self.alloc_slot();
                let len = self.r.read_int()? as usize;
                if class.name != "[B" {
                    return Err(Error::Malformed(format!(
                        "array component '{}' is not supported by this port (only [B)",
                        class.name
                    )));
                }
                let bytes = self.r.read_raw(len)?.to_vec();
                let v = JavaValue::ByteArray { class, bytes };
                self.handles[slot] = HandleSlot::Value(v.clone());
                Ok(v)
            }
            TC_OBJECT => {
                let class = self
                    .read_class_desc_ref()?
                    .ok_or_else(|| Error::Malformed("TC_OBJECT with a null class descriptor".into()))?;
                let slot = self.alloc_slot();
                let chain = ClassDesc::chain_root_first(&class);
                let mut levels = Vec::with_capacity(chain.len());
                for class_at_level in &chain {
                    let mut fields = Vec::with_capacity(class_at_level.fields.len());
                    for f in &class_at_level.fields {
                        fields.push(self.read_field_value(f)?);
                    }
                    let annotation = if class_at_level.has_annotation() {
                        self.read_annotation()?
                    } else {
                        Vec::new()
                    };
                    levels.push(ObjectLevel { fields, annotation });
                }
                let v = JavaValue::Object { class, levels };
                self.handles[slot] = HandleSlot::Value(v.clone());
                Ok(v)
            }
            other => Err(Error::UnknownTag {
                what: "object tag",
                value: other as i64,
            }),
        }
    }

    fn at_eof(&self) -> bool {
        self.r.remaining() == 0
    }
}

/// Decodes every top-level value in a stream (matching however many times `writeObject` was
/// called on the `ObjectOutputStream` that produced it).
pub fn decode_stream(bytes: &[u8]) -> Result<Vec<JavaValue>> {
    decode_stream_impl(bytes, None)
}

/// Same as [`decode_stream`], but every freshly-resolved class descriptor is checked against
/// `allow_list` first (mirrors `CheckedInputStream.resolveClass`,
/// `commons/all/.../marshall/CheckedInputStream.java:22-31`) — a class name that fails the check
/// aborts decoding with [`Error::NotAllowed`], exactly like upstream refuses to resolve it.
pub fn decode_stream_with_allow_list(
    bytes: &[u8],
    allow_list: &ClassAllowList,
) -> Result<Vec<JavaValue>> {
    decode_stream_impl(bytes, Some(allow_list))
}

fn decode_stream_impl(bytes: &[u8], allow_list: Option<&ClassAllowList>) -> Result<Vec<JavaValue>> {
    let mut sr = StreamReader::new(bytes, allow_list)?;
    let mut values = Vec::new();
    while !sr.at_eof() {
        values.push(sr.read_value()?);
    }
    Ok(values)
}

/// Decodes a stream that holds exactly one value.
pub fn decode_one(bytes: &[u8]) -> Result<JavaValue> {
    let mut values = decode_stream(bytes)?;
    if values.len() != 1 {
        return Err(Error::Malformed(format!(
            "expected exactly one top-level value, found {}",
            values.len()
        )));
    }
    Ok(values.remove(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_vectors(path: &str) -> HashMap<String, Vec<u8>> {
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

    fn vectors() -> HashMap<String, Vec<u8>> {
        load_vectors(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/vectors/java_serial.hex"
        ))
    }

    /// The primary byte-exactness proof for every real-Java vector: decode it, re-encode what
    /// was decoded, and require the exact same bytes back — see the module doc for why this is
    /// the chosen strategy.
    fn assert_decode_reencode_byte_exact(label: &str) {
        let vectors = vectors();
        let bytes = vectors.get(label).unwrap_or_else(|| panic!("no vector {label}"));
        let decoded = decode_stream(bytes).unwrap_or_else(|e| panic!("{label}: decode failed: {e}"));
        let reencoded = encode_stream(&decoded);
        assert_eq!(
            reencoded, *bytes,
            "{label}: decode-then-re-encode did not reproduce the original bytes"
        );
    }

    #[test]
    fn string_vectors_round_trip_byte_exact() {
        for label in ["string_hello", "string_empty", "string_non_ascii"] {
            assert_decode_reencode_byte_exact(label);
        }
    }

    #[test]
    fn boxed_long_and_integer_round_trip_byte_exact() {
        for label in [
            "boxed_long",
            "boxed_long_negative",
            "boxed_integer",
            "boxed_integer_zero",
        ] {
            assert_decode_reencode_byte_exact(label);
        }
        // And the decoded shape is sane: Long/Integer are TC_OBJECT with a two-level chain
        // (the boxed type itself, then java.lang.Number), a single 'value' field, no annotation.
        let vectors = vectors();
        let decoded = decode_one(&vectors["boxed_long"]).unwrap();
        match decoded {
            JavaValue::Object { class, levels } => {
                assert_eq!(class.name, "java.lang.Long");
                assert_eq!(class.super_desc.as_ref().unwrap().name, "java.lang.Number");
                assert_eq!(levels.len(), 2);
                assert_eq!(levels[0].fields.len(), 0); // Number: no fields
                assert_eq!(levels[1].fields, vec![FieldValue::Long(42)]);
            }
            other => panic!("expected Object, got {other:?}"),
        }
    }

    #[test]
    fn versioned_wrapper_round_trips_byte_exact() {
        for label in ["versioned_wrapper_string", "versioned_wrapper_null_value"] {
            assert_decode_reencode_byte_exact(label);
        }
        let vectors = vectors();
        let decoded = decode_one(&vectors["versioned_wrapper_string"]).unwrap();
        match decoded {
            JavaValue::Object { class, levels } => {
                assert_eq!(class.name, "org.mockserver.state.infinispan.VersionedWrapper");
                assert_eq!(class.serial_version_uid, 1);
                assert!(class.super_desc.is_none());
                assert_eq!(levels.len(), 1);
                assert_eq!(
                    levels[0].fields,
                    vec![
                        FieldValue::Long(1),
                        FieldValue::Obj(Box::new(JavaValue::Str("hello".into()))),
                    ]
                );
            }
            other => panic!("expected Object, got {other:?}"),
        }
    }

    #[test]
    fn blob_round_trips_byte_exact() {
        for label in ["blob_default_empty_map", "blob_empty_data", "blob_with_metadata"] {
            assert_decode_reencode_byte_exact(label);
        }
        let vectors = vectors();
        let decoded = decode_one(&vectors["blob_default_empty_map"]).unwrap();
        match decoded {
            JavaValue::Object { class, levels } => {
                assert_eq!(class.name, "org.mockserver.state.Blob");
                assert_eq!(levels.len(), 1);
                assert_eq!(levels[0].fields.len(), 3);
                assert_eq!(
                    levels[0].fields[0],
                    FieldValue::Obj(Box::new(JavaValue::ByteArray {
                        class: byte_array_class_desc(),
                        bytes: vec![1, 2, 3, 4],
                    }))
                );
                assert_eq!(
                    levels[0].fields[1],
                    FieldValue::Obj(Box::new(JavaValue::Str("key1".into())))
                );
                match &levels[0].fields[2] {
                    FieldValue::Obj(v) => match v.as_ref() {
                        JavaValue::Object { class, levels } => {
                            assert_eq!(class.name, "java.util.Collections$EmptyMap");
                            assert_eq!(levels[0].fields.len(), 0);
                        }
                        other => panic!("expected Object, got {other:?}"),
                    },
                    other => panic!("expected Obj, got {other:?}"),
                }
            }
            other => panic!("expected Object, got {other:?}"),
        }
    }

    #[test]
    fn node_serialization_round_trips_byte_exact() {
        for label in ["node_serialization", "node_serialization_empty"] {
            assert_decode_reencode_byte_exact(label);
        }
        let vectors = vectors();
        let decoded = decode_one(&vectors["node_serialization"]).unwrap();
        match decoded {
            JavaValue::Object { class, levels } => {
                assert_eq!(class.name, "com.fasterxml.jackson.databind.node.NodeSerialization");
                assert_eq!(class.flags, SC_EXTERNALIZABLE | SC_BLOCK_DATA);
                assert_eq!(levels.len(), 1);
                assert_eq!(levels[0].fields.len(), 0); // Externalizable: no defaultWriteObject fields
                // writeExternal wrote writeInt(len) + raw bytes, as one opaque block.
                assert_eq!(
                    levels[0].annotation,
                    vec![AnnotationItem::Block(vec![0, 0, 0, 2, b'{', b'}'])]
                );
            }
            other => panic!("expected Object, got {other:?}"),
        }
    }

    #[test]
    fn expectation_entry_stand_in_round_trips_byte_exact() {
        for label in ["expectation_entry_unlimited", "expectation_entry_limited"] {
            assert_decode_reencode_byte_exact(label);
        }
        let vectors = vectors();
        let decoded = decode_one(&vectors["expectation_entry_unlimited"]).unwrap();
        match decoded {
            JavaValue::Object { class, levels } => {
                assert_eq!(class.name, "org.mockserver.state.ExpectationEntry");
                assert_eq!(class.serial_version_uid, 2);
                assert_eq!(class.flags, SC_SERIALIZABLE | SC_WRITE_METHOD);
                assert_eq!(levels.len(), 1);
                // Declared field order: created(J), priority(I), remainingTimes(I), id(L) — the
                // JDK's own primitives-then-objects field ordering.
                assert_eq!(
                    levels[0].fields,
                    vec![
                        FieldValue::Long(1000),
                        FieldValue::Int(0),
                        FieldValue::Int(-1),
                        FieldValue::Obj(Box::new(JavaValue::Str("id-1".into()))),
                    ]
                );
                // The custom writeObject's extra writeUTF(json) landed as block-data annotation.
                assert_eq!(levels[0].annotation.len(), 1);
            }
            other => panic!("expected Object, got {other:?}"),
        }
    }

    #[test]
    fn handle_back_references_round_trip_byte_exact() {
        // Class-descriptor back-references decode-then-re-encode byte-exact: the writer
        // deduplicates class descriptors by name unconditionally (see `write_class_desc`), so it
        // doesn't matter that decoding a TC_REFERENCE loses the "this was a reference, not a
        // fresh TC_CLASSDESC" distinction — re-encoding rediscovers the need for one on its own.
        assert_decode_reencode_byte_exact("versioned_wrapper_classdesc_backref");

        let vectors = vectors();
        let decoded = decode_stream(&vectors["versioned_wrapper_classdesc_backref"]).unwrap();
        assert_eq!(decoded.len(), 2);
        match (&decoded[0], &decoded[1]) {
            (JavaValue::Object { class: c1, .. }, JavaValue::Object { class: c2, .. }) => {
                assert!(Rc::ptr_eq(c1, c2), "second object should reuse the first's class descriptor");
            }
            other => panic!("expected two Objects, got {other:?}"),
        }

        // A plain *value*-level back-reference (two writeObject calls of the very same String
        // instance) is different: decoding it necessarily loses Java's object-identity
        // information (there is no Rust equivalent to compare against — see `JavaValue::SameAs`'s
        // doc comment), so decode-then-re-encode cannot reproduce a value-level TC_REFERENCE.
        // That is proven from the *write* side instead, immediately below.
        let decoded = decode_stream(&vectors["string_backref"]).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], decoded[1]); // both resolve to the same String value
    }

    #[test]
    fn string_value_back_reference_encodes_byte_exact_via_same_as() {
        // The write-side counterpart of the note above: JavaValue::SameAs lets a caller who does
        // know two values share identity (as this crate's own construction code always will,
        // since it never has two independently-allocated Rust values that are secretly "the same
        // Java object") produce the exact same TC_REFERENCE bytes real Java does.
        let vectors = vectors();
        let bytes = encode_stream(&[
            JavaValue::Str("shared-instance".into()),
            JavaValue::SameAs(0),
        ]);
        assert_eq!(bytes, vectors["string_backref"]);
    }

    #[test]
    fn write_side_reuses_class_descriptor_and_produces_a_reference() {
        // Not from a fixture: proves the *writer* also deduplicates class descriptors by name,
        // by constructing two VersionedWrapper-shaped values from scratch (no decode involved).
        let class = Rc::new(ClassDesc {
            name: "org.mockserver.state.infinispan.VersionedWrapper".into(),
            serial_version_uid: 1,
            flags: SC_SERIALIZABLE,
            fields: vec![
                FieldDesc::primitive(b'J', "version"),
                FieldDesc::object("value", "Ljava/lang/Object;"),
            ],
            super_desc: None,
        });
        let make = |version: i64, value: &str| JavaValue::Object {
            class: class.clone(),
            levels: vec![ObjectLevel {
                fields: vec![
                    FieldValue::Long(version),
                    FieldValue::Obj(Box::new(JavaValue::Str(value.into()))),
                ],
                annotation: vec![],
            }],
        };
        let bytes = encode_stream(&[make(1, "a"), make(2, "b")]);

        // The second object's classDesc must be TC_REFERENCE, not a second TC_CLASSDESC: there
        // is exactly one occurrence of the class name's UTF-8 bytes in the stream.
        let name_bytes = b"org.mockserver.state.infinispan.VersionedWrapper";
        let occurrences = bytes
            .windows(name_bytes.len())
            .filter(|w| *w == &name_bytes[..])
            .count();
        assert_eq!(occurrences, 1, "class name should appear exactly once");

        let decoded = decode_stream(&bytes).unwrap();
        assert_eq!(decoded.len(), 2);
        if let (JavaValue::Object { class: c1, .. }, JavaValue::Object { class: c2, .. }) =
            (&decoded[0], &decoded[1])
        {
            assert!(Rc::ptr_eq(c1, c2));
        } else {
            panic!("expected two Objects");
        }
    }

    #[test]
    fn eof_and_malformed_input_are_reported_not_panicked() {
        assert!(decode_stream(&[]).is_err());
        assert!(decode_stream(&[0xac, 0xed, 0x00, 0x05, 0x99]).is_err()); // unknown tag 0x99
    }

    // --- ported from DeserializationAllowListTest.AllowListEnforcementTest's two "full
    //     marshaller round trip" cases (the other 8 allow + 5 reject cases are in
    //     allow_list.rs, since they only exercise ClassAllowList::is_safe_class directly). ---

    fn clustered_allow_list() -> ClassAllowList {
        ClassAllowList::new(
            std::iter::empty(),
            [
                r"org\.mockserver\.state\.infinispan\..*",
                r"org\.mockserver\.state\..*",
                r"org\.mockserver\.mock\..*",
                r"org\.mockserver\.model\..*",
                r"org\.mockserver\.matchers\..*",
                r"com\.fasterxml\.jackson\..*",
                r"java\.lang\..*",
                r"java\.util\..*",
                r"java\.time\..*",
                r"\[B",
            ]
            .map(String::from),
        )
    }

    #[test]
    fn should_marshal_and_unmarshal_allowed_type() {
        // shouldMarshalAndUnmarshalAllowedType: a VersionedWrapper (allow-listed by
        // `org.mockserver.state.infinispan.*`) round-trips under the tight allow-list.
        let allow_list = clustered_allow_list();
        let vectors = vectors();
        let decoded =
            decode_stream_with_allow_list(&vectors["versioned_wrapper_string"], &allow_list)
                .expect("VersionedWrapper is allow-listed");
        assert_eq!(decoded.len(), 1);
    }

    #[test]
    fn should_reject_disallowed_type_during_deserialization() {
        // shouldRejectDisallowedTypeDuringDeserialization: a java.net.URL-shaped object (real
        // URL serialization is far deeper than this port needs — `java.net` is simply not on
        // the clustered allow-list, which is the only thing under test here) must be refused.
        let disallowed_class = Rc::new(ClassDesc {
            name: "java.net.URL".into(),
            serial_version_uid: -7627629688361524110,
            flags: SC_SERIALIZABLE,
            fields: vec![FieldDesc::object("host", "Ljava/lang/String;")],
            super_desc: None,
        });
        let value = JavaValue::Object {
            class: disallowed_class,
            levels: vec![ObjectLevel {
                fields: vec![FieldValue::Obj(Box::new(JavaValue::Str(
                    "example.com".into(),
                )))],
                annotation: vec![],
            }],
        };
        let bytes = encode_stream(&[value]);

        let allow_list = clustered_allow_list();
        let err = decode_stream_with_allow_list(&bytes, &allow_list).unwrap_err();
        match err {
            Error::NotAllowed(name) => assert_eq!(name, "java.net.URL"),
            other => panic!("expected NotAllowed, got {other:?}"),
        }
    }
}
