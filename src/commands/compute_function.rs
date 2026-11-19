//! The function a `ComputeCommand` carries. `EncoderCache.compute` wraps the user's lambda in a
//! `BiFunctionMapper` (`cache/impl/EncoderCache.java:1123-1127`, externalizer id 126,
//! `BiFunctionMapper.java:59-83`) that writes the lambda, then the key and value `DataConversion`s
//! (`encoding/DataConversion.java:353-374`); the lambda itself goes through `LambdaMarshaller`
//! (`marshall/core/LambdaMarshaller.java:29-43`, `GlobalMarshaller.ID_LAMBDA`).
//!
//! A Rust node cannot run a Java lambda. It recognises the one lambda MockServer sends
//! (`InfinispanKeyValueStore.put`, `InfinispanKeyValueStore.java:58-61`: the descriptor in
//! `tests/vectors/write_commands.hex`, captured from the real class) by its `SerializedLambda`
//! description and runs the Rust equivalent. A descriptor it does not know fails the command.

use std::sync::Arc;

use crate::cache::versioned::VersionedWrapper;
use crate::marshall::error::{Error, Result};
use crate::marshall::marshall_util::{marshall_size, unmarshall_size};
use crate::topology::wire::{ID_INTERNAL, ObjectInput, ObjectOutput};

use super::object::CacheObject;

/// The externalizer id of `BiFunctionMapper`.
pub const ID_BI_FUNCTION_MAPPER: u8 = 126;
const ID_LAMBDA: u8 = 0x08;
const ID_ANNOTATED: u8 = 0x04;
const ID_CLASS: u8 = 144;
const MEDIA_TYPE_EXTERNALIZER: &str = "org.infinispan.commons.dataconversion.MediaType$MediaTypeExternalizer";

/// The class whose lambda MockServer's Infinispan state store sends: `InfinispanKeyValueStore`.
pub const MOCKSERVER_STORE_CLASS: &str = "org/mockserver/state/infinispan/InfinispanKeyValueStore";
/// `SerializableBiFunction`, the functional interface of that lambda.
pub const SERIALIZABLE_BI_FUNCTION: &str = "org/infinispan/util/function/SerializableBiFunction";
/// `lambda$put$<hash>$<n>`: javac suffixes serialisable lambdas with a hash of their shape.
pub const PUT_LAMBDA_PREFIX: &str = "lambda$put$";
const PUT_LAMBDA_IMPL_SIGNATURE: &str =
    "(Ljava/lang/Object;Ljava/lang/String;Lorg/mockserver/state/infinispan/VersionedWrapper;)Lorg/mockserver/state/infinispan/VersionedWrapper;";
const PUT_LAMBDA_INSTANTIATED: &str =
    "(Ljava/lang/String;Lorg/mockserver/state/infinispan/VersionedWrapper;)Lorg/mockserver/state/infinispan/VersionedWrapper;";
/// The suffix javac computed for `InfinispanKeyValueStore.put`'s lambda (from the captured vector).
pub const PUT_LAMBDA_NAME: &str = "lambda$put$ecf05865$1";

/// `java.lang.invoke.SerializedLambda` as `LambdaMarshaller` writes it, with captured arguments that
/// can be user objects.
#[derive(Debug, Clone, PartialEq)]
pub struct CacheLambda {
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
    /// The implementation method's name, `lambda$put$...`.
    pub impl_method_name: String,
    /// The implementation method's descriptor.
    pub impl_method_signature: String,
    /// The descriptor of the interface method as instantiated.
    pub instantiated_method_type: String,
    /// The values the lambda captured.
    pub captured_args: Vec<CacheObject>,
}

impl CacheLambda {
    /// The `InfinispanKeyValueStore.put` lambda capturing `value`.
    pub fn mockserver_put(value: CacheObject) -> Self {
        Self {
            capturing_class: MOCKSERVER_STORE_CLASS.into(),
            functional_interface_class: SERIALIZABLE_BI_FUNCTION.into(),
            functional_interface_method_name: "apply".into(),
            functional_interface_method_signature: "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;".into(),
            impl_method_kind: 6, // REF_invokeStatic
            impl_class: MOCKSERVER_STORE_CLASS.into(),
            impl_method_name: PUT_LAMBDA_NAME.into(),
            impl_method_signature: PUT_LAMBDA_IMPL_SIGNATURE.into(),
            instantiated_method_type: PUT_LAMBDA_INSTANTIATED.into(),
            captured_args: vec![value],
        }
    }

    /// Run the Rust equivalent: `(k, existing) -> new VersionedWrapper<>(value, existing != null ?
    /// existing.getVersion() + 1 : 1L)`. Returns the new value (never null for this lambda).
    pub fn apply(&self, existing: Option<&CacheObject>) -> Result<Option<CacheObject>> {
        let recognised = self.capturing_class == MOCKSERVER_STORE_CLASS
            && self.impl_class == MOCKSERVER_STORE_CLASS
            && self.impl_method_name.starts_with(PUT_LAMBDA_PREFIX)
            && self.impl_method_signature == PUT_LAMBDA_IMPL_SIGNATURE
            && self.captured_args.len() == 1;
        if !recognised {
            return Err(Error::Malformed(format!(
                "no Rust equivalent for lambda {}.{}{}",
                self.impl_class, self.impl_method_name, self.impl_method_signature
            )));
        }
        let version = match existing {
            Some(CacheObject::User(u)) => VersionedWrapper::decode(u.bytes())?.version + 1,
            Some(CacheObject::Null) | None => 1,
            Some(other) => return Err(Error::Malformed(format!("existing value is not a VersionedWrapper: {other:?}"))),
        };
        Ok(Some(CacheObject::User(VersionedWrapper::encode_object(&self.captured_args[0], version)?)))
    }
}

/// `MediaType` as `MediaTypeExternalizer` writes it (no parameters: a parameterised media type is not modelled).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaType {
    /// `MediaTypeIds` id
    Internal(i16),
    /// A media type by its name.
    Custom(String),
}

/// `DataConversion.writeTo`/`readFrom`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataConversion {
    /// Whether it converts keys rather than values.
    pub is_key: bool,
    /// The encoder's id.
    pub encoder_id: i16,
    /// The wrapper's id; `0` for none.
    pub wrapper_id: i8,
    /// The media type the caller reads and writes.
    pub request_media_type: MediaType,
}

impl DataConversion {
    /// The conversion a default (object storage) cache uses: `IdentityEncoder` (id 1) and no wrapper,
    /// request media type `MediaTypeIds` 1 (taken from the vectors).
    pub fn identity(is_key: bool) -> Self {
        Self { is_key, encoder_id: 1, wrapper_id: 0, request_media_type: MediaType::Internal(1) }
    }
}

/// `BiFunctionMapper`: the lambda plus the cache's key and value conversions.
#[derive(Debug, Clone, PartialEq)]
pub struct BiFunctionMapper {
    /// The remapping function.
    pub function: CacheLambda,
    /// How keys are converted.
    pub key_conversion: DataConversion,
    /// How values are converted.
    pub value_conversion: DataConversion,
}

impl BiFunctionMapper {
    /// The mapper of `function` for a default cache: identity conversions for keys and values.
    pub fn for_default_cache(function: CacheLambda) -> Self {
        Self { function, key_conversion: DataConversion::identity(true), value_conversion: DataConversion::identity(false) }
    }
}

impl ObjectOutput {
    fn write_data_conversion(&mut self, dc: &DataConversion) {
        self.w.write_byte(if dc.is_key { 2 } else { 0 });
        self.w.write_short(dc.encoder_id);
        self.w.write_byte(dc.wrapper_id);
        // writeObject(MediaType): ID_ANNOTATED, writeObject(externalizer class), externalizer body
        self.w.write_unsigned_byte(ID_ANNOTATED);
        self.write_internal_header(ID_CLASS);
        self.w.write_utf(MEDIA_TYPE_EXTERNALIZER);
        match &dc.request_media_type {
            MediaType::Internal(id) => {
                self.w.write_boolean(true);
                self.w.write_short(*id);
            }
            MediaType::Custom(s) => {
                self.w.write_boolean(false);
                self.w.write_utf(s);
            }
        }
        // writeObject(params): the empty map (`MapExternalizer.EMPTYMAP`)
        self.write_internal_header(crate::topology::wire::ID_MAPS);
        self.w.write_unsigned_byte(7);
    }

    /// `LambdaMarshaller.write`: writes `l` as a `SerializedLambda`.
    pub fn write_lambda(&mut self, l: &CacheLambda) {
        self.w.write_unsigned_byte(ID_LAMBDA);
        self.w.write_utf(&l.capturing_class);
        self.w.write_utf(&l.functional_interface_class);
        self.w.write_utf(&l.functional_interface_method_name);
        self.w.write_utf(&l.functional_interface_method_signature);
        self.w.write_int(l.impl_method_kind);
        self.w.write_utf(&l.impl_class);
        self.w.write_utf(&l.impl_method_name);
        self.w.write_utf(&l.impl_method_signature);
        self.w.write_utf(&l.instantiated_method_type);
        marshall_size(&mut self.w, l.captured_args.len() as i32);
        for a in &l.captured_args {
            self.write_cache_object(a);
        }
    }

    /// `writeObject(BiFunctionMapper)`.
    pub fn write_bi_function_mapper(&mut self, m: &BiFunctionMapper) {
        self.write_internal_header(ID_BI_FUNCTION_MAPPER);
        self.write_lambda(&m.function);
        self.write_data_conversion(&m.key_conversion);
        self.write_data_conversion(&m.value_conversion);
    }
}

impl ObjectInput<'_> {
    fn read_data_conversion(&mut self) -> Result<DataConversion> {
        let flags = self.r.read_byte()?;
        let encoder_id = self.r.read_short()?;
        let wrapper_id = self.r.read_byte()?;
        if self.r.read_unsigned_byte()? != ID_ANNOTATED {
            return Err(Error::Malformed("DataConversion media type is not annotated".into()));
        }
        self.expect_internal(ID_CLASS, "Class header")?;
        let ext = self.r.read_utf()?;
        if ext != MEDIA_TYPE_EXTERNALIZER {
            return Err(Error::Malformed(format!("unexpected media type externalizer {ext}")));
        }
        let request_media_type = if self.r.read_boolean()? {
            MediaType::Internal(self.r.read_short()?)
        } else {
            MediaType::Custom(self.r.read_utf()?)
        };
        self.expect_internal(crate::topology::wire::ID_MAPS, "Map header")?;
        if self.r.read_unsigned_byte()? != 7 {
            return Err(Error::Malformed("media type parameters are not modelled".into()));
        }
        Ok(DataConversion { is_key: flags & 2 == 2, encoder_id, wrapper_id, request_media_type })
    }

    /// `LambdaMarshaller.read`: reads a `SerializedLambda`.
    pub fn read_lambda(&mut self) -> Result<CacheLambda> {
        let tag = self.r.read_unsigned_byte()?;
        if tag != ID_LAMBDA {
            return Err(Error::UnknownTag { what: "lambda type byte", value: tag as i64 });
        }
        let capturing_class = self.r.read_utf()?;
        let functional_interface_class = self.r.read_utf()?;
        let functional_interface_method_name = self.r.read_utf()?;
        let functional_interface_method_signature = self.r.read_utf()?;
        let impl_method_kind = self.r.read_int()?;
        let impl_class = self.r.read_utf()?;
        let impl_method_name = self.r.read_utf()?;
        let impl_method_signature = self.r.read_utf()?;
        let instantiated_method_type = self.r.read_utf()?;
        let n = unmarshall_size(&mut self.r)?;
        let captured_args = (0..n.max(0)).map(|_| self.read_cache_object()).collect::<Result<Vec<_>>>()?;
        Ok(CacheLambda {
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

    /// `readObject()` for a `BiFunctionMapper` (the only function a compute command carries here).
    pub fn read_bi_function_mapper(&mut self) -> Result<BiFunctionMapper> {
        if self.r.read_unsigned_byte()? != ID_INTERNAL {
            return Err(Error::Malformed("compute function is not an internal object".into()));
        }
        let id = self.r.read_unsigned_byte()?;
        if id != ID_BI_FUNCTION_MAPPER {
            return Err(Error::UnknownTag { what: "compute function externalizer id", value: id as i64 });
        }
        let function = self.read_lambda()?;
        let key_conversion = self.read_data_conversion()?;
        let value_conversion = self.read_data_conversion()?;
        Ok(BiFunctionMapper { function, key_conversion, value_conversion })
    }
}

/// Shared handle so commands stay cheap to clone.
pub type SharedMapper = Arc<BiFunctionMapper>;
