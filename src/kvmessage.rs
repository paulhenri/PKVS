use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum KvMessage {
    Set(String, String),
    Get(String),
    Remove(String),
    Response(String),
}
