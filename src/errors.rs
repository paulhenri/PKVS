use color_eyre::Report;
use serde_json::error::Error;

/// KvsError: Enum to deal with error programm wide
#[derive(Debug)]
pub enum KvsError {
    /// Key was not found
    KeyNotFound,
    /// No key was given in the command line
    KeyNotTyped,
    /// Wrapper for io errors
    Io(std::io::Error),

    /// wrapper for serde errors
    Serialize(serde_json::error::Error),

    /// Corrupt file or else - Need to be refactored or refined
    OtherError,

    /// If no value for the size of the record is found
    NoValueOfSize,

    /// If the offset stored in the index map is below zero
    OffsetSubZero,

    /// Errors from the color_eyere library
    EyreError(color_eyre::Report),
}
impl From<std::io::Error> for KvsError {
    fn from(err: std::io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

impl From<color_eyre::Report> for KvsError {
    fn from(err: color_eyre::Report) -> KvsError {
        KvsError::EyreError(err)
    }
}

impl From<serde_json::error::Error> for KvsError {
    fn from(err: serde_json::error::Error) -> KvsError {
        KvsError::Serialize(err)
    }
}

/// Result<T>
pub type Result<T> = std::result::Result<T, KvsError>;

#[derive(Debug)]
pub enum KvsStorageEngine {
    KvsEngine,
    Sled,
}
