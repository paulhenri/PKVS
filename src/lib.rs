#![warn(missing_docs)]
//! Kvs Crate - Three part system that holds :
//! A key-value storage engine (Very basic one)
//! A network server
//! A network client

/// Errors structure module
pub mod errors;
/// Network message module
pub mod kvmessage;
/// Engine module
pub mod kvsengine;
/// Server structure module
pub mod kvsserver;

/// To redistributes errors;
pub use errors::{KvsError, Result};
