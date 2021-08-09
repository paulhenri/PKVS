use serde::{Deserialize, Serialize};

/// Enum used to communicate between client and server
#[derive(Serialize, Deserialize, Debug)]
pub enum KvMessage {
    /// To set value in the data-store
    Set(String, String),
    /// To get value from the data-store
    Get(String),
    ///To remove value from the datastore
    Remove(String),
    /// Response sent by the server to client
    Response(String),
}
