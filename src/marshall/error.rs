//! Error type shared by every codec in `marshall`.

use std::fmt;

/// Why a value could not be marshalled or unmarshalled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Ran out of bytes reading a fixed-size or length-prefixed value.
    Eof,
    /// A framing byte (type tag, marker, protocol constant) had a value the codec does not
    /// recognise.
    UnknownTag {
        /// What the byte was read as.
        what: &'static str,
        /// The value read.
        value: i64,
    },
    /// Malformed content that isn't just running out of bytes (bad modified-UTF-8, a handle
    /// pointing past what has been written so far, etc).
    Malformed(String),
    /// A class name failed the deserialization allow-list check
    /// (`org.infinispan.commons.marshall.CheckedInputStream.resolveClass`).
    NotAllowed(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Eof => write!(f, "unexpected end of input"),
            Error::UnknownTag { what, value } => write!(f, "unknown {what}: {value:#x}"),
            Error::Malformed(msg) => write!(f, "malformed input: {msg}"),
            Error::NotAllowed(class_name) => {
                write!(f, "class '{class_name}' not in allowlist")
            }
        }
    }
}

impl std::error::Error for Error {}

/// The result of a codec.
pub type Result<T> = std::result::Result<T, Error>;
