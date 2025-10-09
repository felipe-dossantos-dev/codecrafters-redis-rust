use std::collections::VecDeque;

use crate::{
    resp::RespDataType,
    values::{key_value::KeyValue, sorted_set::SortedSet},
};

pub mod key_value;
pub mod sorted_set;

#[derive(Debug, PartialEq)]
pub enum RedisValue {
    None,
    String(KeyValue),
    List(VecDeque<String>),
    ZSet(SortedSet),
    // Set,
    // Hash,
    // Stream,
    // VectorSet,
}

impl RedisValue {
    pub fn to_type_resp(&self) -> RespDataType {
        match self {
            RedisValue::None => RespDataType::simple_string("none"),
            RedisValue::String(_) => RespDataType::simple_string("string"),
            RedisValue::List(_) => RespDataType::simple_string("list"),
            RedisValue::ZSet(_) => RespDataType::simple_string("zset"),
            // DataType::Set => RespDataType::simple_string("set"),
            // DataType::Hash => RespDataType::simple_string("hash"),
            // DataType::Stream => RespDataType::simple_string("stream"),
            // DataType::VectorSet => RespDataType::simple_string("vectorset"),
        }
    }
}
