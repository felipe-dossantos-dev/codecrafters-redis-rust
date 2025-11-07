use std::collections::VecDeque;

use crate::{
    resp::RespDataType,
    types::{key_value::KeyValue, sorted_set::SortedSet, stream::RedisStream},
};

pub mod key_value;
pub mod sorted_set;
pub mod stream;

#[derive(Debug, PartialEq)]
pub enum RedisType {
    None,
    String(KeyValue),
    List(VecDeque<String>),
    ZSet(SortedSet),
    // Set,
    // Hash,
    Stream(RedisStream),
    // VectorSet,
}

impl RedisType {
    pub fn to_type_resp(&self) -> RespDataType {
        match self {
            RedisType::None => RespDataType::simple_string("none"),
            RedisType::String(_) => RespDataType::simple_string("string"),
            RedisType::List(_) => RespDataType::simple_string("list"),
            RedisType::ZSet(_) => RespDataType::simple_string("zset"),
            RedisType::Stream(_) => RespDataType::simple_string("stream"),
            // DataType::Set => RespDataType::simple_string("set"),
            // DataType::Hash => RespDataType::simple_string("hash"),
            // DataType::VectorSet => RespDataType::simple_string("vectorset"),
        }
    }
}
