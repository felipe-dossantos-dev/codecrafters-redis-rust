use crate::resp::RespDataType;

pub mod sorted_set;

#[derive(Debug, PartialEq)]
pub enum DataType {
    None,
    String,
    // List,
    // Set,
    // ZSet,
    // Hash,
    // Stream,
    // VectorSet,
}

impl DataType {
    pub fn to_resp(&self) -> RespDataType {
        match self {
            DataType::None => RespDataType::simple_string("none"),
            DataType::String => RespDataType::simple_string("string"),
            // DataType::List => RespDataType::simple_string("list"),
            // DataType::Set => RespDataType::simple_string("set"),
            // DataType::ZSet => RespDataType::simple_string("zset"),
            // DataType::Hash => RespDataType::simple_string("hash"),
            // DataType::Stream => RespDataType::simple_string("stream"),
            // DataType::VectorSet => RespDataType::simple_string("vectorset"),
        }
    }
}
