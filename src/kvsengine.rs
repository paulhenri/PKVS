pub use crate::Result;
/// KvStore
pub mod kvstore;
pub use kvstore::KvStore;

/// KvsEngine trait used if we wanted to implemet new storage engine
pub trait KvsEngine {
    /// set function prototype
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// get function prototype
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// remove function prototype
    fn remove(&mut self, key: String) -> Result<()>;
}
