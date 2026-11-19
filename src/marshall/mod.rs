//! `GlobalMarshaller` and the codecs it is built from (`core/src/main/java/org/infinispan/
//! marshall/` and `commons/all/.../marshall/`, Infinispan 14.0.35.Final; `WrappedMessage`,
//! ProtoStream 4.6.5.Final). See each submodule's doc comment for the exact upstream files it
//! ports and how its test vectors were produced.

pub mod allow_list;
pub mod error;
pub mod global;
pub mod java_serial;
pub mod lambda;
pub mod marshall_util;
pub mod primitives;
pub mod wrapped_message;

pub use error::{Error, Result};
