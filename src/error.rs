use crate::Value;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvError {
    #[error("Not found for table: {0}, key: {1}")]
    NotFound(String, String),

    #[error("Cannot parse command: `{0}`")]
    InvalidCommand(String),
    #[error("Cannot convert value {:0} to {1}")]
    ConvertError(Value, &'static str),
    #[error("Cannot process command {0} with table: {1}, key: {2}. Error: {3}")]
    StorageError(&'static str, String, String, String),

    #[error("Failed to encode protobuf message")]
    EncodeError(#[from] prost::EncodeError),
    #[error("Failed to decode protobuf message")]
    DecodeError(#[from] prost::DecodeError),
    #[error("Failed to access sled db")]
    SledError(#[from] sled::Error),
    #[error("Failed to access rocksdb")]
    RocksDBError(#[from] rocksdb::Error),

    #[error("Failed to parse certifcate: {0}, {1}")]
    CertifcateParseError(&'static str, &'static str),

    #[error("Frame is larger than max size!")]
    FrameError,

    #[error("TLS Error")]
    TLSError(#[from] tokio_rustls::rustls::TLSError),

    #[error("I/O Error")]
    IoError(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    Internal(String),
}

// impl From<sled::Error> for KvError {
//     fn from(_: sled::Error) -> Self {
//         Self::Internal(String::from("error with sleddb"))
//     }
// }