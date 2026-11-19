//! Wire format for `GlobalMarshaller`'s `ID_LAMBDA` path
//! (`core/src/main/java/org/infinispan/marshall/core/LambdaMarshaller.java`, 14.0.35.Final):
//! nine `writeUTF` strings describing a `java.lang.invoke.SerializedLambda`, then the impl-method
//! kind and the captured-argument list.
//!
//! **Scope note:** this ports the *encode/decode format* only — a
//! byte-exact round trip of a [`LambdaDescriptor`] value. It deliberately does **not** attempt to
//! recover or run `InfinispanKeyValueStore.put`'s real captured `SerializedLambda` strings
//! (`implMethodName` etc.): those can only come from a golden capture off a running
//! oracle+tapping-proxy. Running the received descriptor on the Rust side is the
//! `commands::compute_function` module's job, not this one's (`LambdaMarshaller.java:45-54`,
//! `$deserializeLambda$`).
//!
//! Uses `MarshallUtil.marshallSize`/`unmarshallSize` ([`super::marshall_util`]) for the
//! captured-argument count, and `GlobalMarshaller`'s own nullable-object dispatch
//! ([`super::global`]) for each captured argument — matching `LambdaMarshaller.write`/`read`
//! (`:29-43`, `:56-75`) exactly, restricted to [`super::global::Value`]'s current scope (see that
//! module's doc for what it can carry).

use super::error::Result;
use super::global::{self, Value};
use super::marshall_util::{marshall_size, unmarshall_size};
use super::primitives::{ByteReader, ByteWriter};

/// The nine `SerializedLambda` accessor strings plus `implMethodKind`, mirroring
/// `LambdaMarshaller.writeSerializedLambda` (`:29-43`) field for field.
#[derive(Debug, Clone, PartialEq)]
pub struct LambdaDescriptor {
    /// The class the lambda was written in.
    pub capturing_class: String,
    /// The functional interface the lambda implements.
    pub functional_interface_class: String,
    /// The interface method's name.
    pub functional_interface_method_name: String,
    /// The interface method's descriptor.
    pub functional_interface_method_signature: String,
    /// The `MethodHandleInfo` kind of the implementation method.
    pub impl_method_kind: i32,
    /// The class holding the implementation method.
    pub impl_class: String,
    /// The implementation method's name.
    pub impl_method_name: String,
    /// The implementation method's descriptor.
    pub impl_method_signature: String,
    /// The descriptor of the interface method as instantiated.
    pub instantiated_method_type: String,
    /// The values the lambda captured.
    pub captured_args: Vec<Value>,
}

/// `LambdaMarshaller.write`'s body minus the `ID_LAMBDA` type-byte prefix (that byte is
/// `GlobalMarshaller`'s job, once `Value` grows an `ID_LAMBDA` variant — see [`super::global`]'s
/// scope note); this is the `writeSerializedLambda` payload itself.
pub fn write(w: &mut ByteWriter, d: &LambdaDescriptor) {
    w.write_utf(&d.capturing_class);
    w.write_utf(&d.functional_interface_class);
    w.write_utf(&d.functional_interface_method_name);
    w.write_utf(&d.functional_interface_method_signature);
    w.write_int(d.impl_method_kind);
    w.write_utf(&d.impl_class);
    w.write_utf(&d.impl_method_name);
    w.write_utf(&d.impl_method_signature);
    w.write_utf(&d.instantiated_method_type);
    marshall_size(w, d.captured_args.len() as i32);
    for arg in &d.captured_args {
        global::write_nullable(w, arg);
    }
}

/// `LambdaMarshaller.createSerializedLambda`'s body (`:56-75`), minus the `Class.forName`/
/// `$deserializeLambda$` invocation half of `LambdaMarshaller.read` (that lives in
/// `commands::compute_function`, see the module doc).
pub fn read(r: &mut ByteReader) -> Result<LambdaDescriptor> {
    let capturing_class = r.read_utf()?;
    let functional_interface_class = r.read_utf()?;
    let functional_interface_method_name = r.read_utf()?;
    let functional_interface_method_signature = r.read_utf()?;
    let impl_method_kind = r.read_int()?;
    let impl_class = r.read_utf()?;
    let impl_method_name = r.read_utf()?;
    let impl_method_signature = r.read_utf()?;
    let instantiated_method_type = r.read_utf()?;
    let n = unmarshall_size(r)?;
    let mut captured_args = Vec::with_capacity(n.max(0) as usize);
    for _ in 0..n {
        captured_args.push(global::read_nullable(r)?);
    }
    Ok(LambdaDescriptor {
        capturing_class,
        functional_interface_class,
        functional_interface_method_name,
        functional_interface_method_signature,
        impl_method_kind,
        impl_class,
        impl_method_name,
        impl_method_signature,
        instantiated_method_type,
        captured_args,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marshall::primitives::PrimitiveValue;

    fn sample() -> LambdaDescriptor {
        // Synthetic values only — see the module doc for why the real captured strings for
        // InfinispanKeyValueStore's put lambda aren't here.
        LambdaDescriptor {
            capturing_class: "org/mockserver/state/infinispan/InfinispanKeyValueStore".into(),
            functional_interface_class: "org/infinispan/util/function/SerializableBiFunction".into(),
            functional_interface_method_name: "apply".into(),
            functional_interface_method_signature: "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;".into(),
            impl_method_kind: 6, // MethodHandleInfo.REF_invokeStatic
            impl_class: "org/mockserver/state/infinispan/InfinispanKeyValueStore".into(),
            impl_method_name: "lambda$put$0".into(),
            impl_method_signature: "(Ljava/lang/Object;Lorg/mockserver/state/infinispan/VersionedWrapper;)Lorg/mockserver/state/infinispan/VersionedWrapper;".into(),
            instantiated_method_type: "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;".into(),
            captured_args: vec![Value::Primitive(PrimitiveValue::Str("captured-value".into()))],
        }
    }

    #[test]
    fn round_trips() {
        let d = sample();
        let mut w = ByteWriter::new();
        write(&mut w, &d);
        let bytes = w.into_bytes();
        let mut r = ByteReader::new(&bytes);
        let decoded = read(&mut r).unwrap();
        assert_eq!(decoded, d);
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn round_trips_with_no_captured_args() {
        let mut d = sample();
        d.captured_args.clear();
        let mut w = ByteWriter::new();
        write(&mut w, &d);
        let bytes = w.into_bytes();
        let mut r = ByteReader::new(&bytes);
        let decoded = read(&mut r).unwrap();
        assert_eq!(decoded, d);
    }

    #[test]
    fn round_trips_with_null_captured_arg() {
        let mut d = sample();
        d.captured_args = vec![Value::Null];
        let mut w = ByteWriter::new();
        write(&mut w, &d);
        let bytes = w.into_bytes();
        let mut r = ByteReader::new(&bytes);
        let decoded = read(&mut r).unwrap();
        assert_eq!(decoded, d);
    }
}
