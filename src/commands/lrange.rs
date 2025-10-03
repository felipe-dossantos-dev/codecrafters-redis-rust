
use super::traits::ParseableCommand;
use crate::types::RedisType;
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct LRangeCommand {
    pub key: String,
    pub start: i64,
    pub end: i64,
}

impl ParseableCommand for LRangeCommand {
    fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "LRANGE command requires a key")?;

        let start_arg = args
            .next()
            .ok_or_else(|| "Expected values for LRANGE start and end".to_string())?;
        let end_arg = args
            .next()
            .ok_or_else(|| "Expected values for LRANGE start and end".to_string())?;

        let start = start_arg
            .to_int()
            .ok_or_else(|| "Expected integer values for LRANGE start and end".to_string())?;
        let end = end_arg
            .to_int()
            .ok_or_else(|| "Expected integer values for LRANGE start and end".to_string())?;

        Ok(LRangeCommand { key, start, end })
    }
}

impl LRangeCommand {
    pub fn treat_bounds(&mut self, list_len: i64) -> Option<(usize, usize)> {
        if self.start > list_len {
            return None;
        }

        if self.start < 0 {
            self.start += list_len
        }

        let start = self.start.max(0) as usize;

        if self.end < 0 {
            self.end += list_len
        }

        if self.end > list_len {
            self.end = list_len - 1
        };

        let end = self.end.max(0) as usize;
        if self.start > self.end {
            return None;
        }
        Some((start, end))
    }
}