use super::traits::ParseableCommand;
use crate::types::RedisType;
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct ZRangeCommand {
    pub key: String,
    pub start: i64,
    pub end: i64,
}

impl ParseableCommand for ZRangeCommand {
    fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "ZRANGE command requires a key")?;

        let start_arg = args
            .next()
            .ok_or_else(|| "Expected values for ZRANGE start and end".to_string())?;
        let end_arg = args
            .next()
            .ok_or_else(|| "Expected values for ZRANGE start and end".to_string())?;

        let start = start_arg
            .to_int()
            .ok_or_else(|| "Expected integer values for ZRANGE start and end".to_string())?;
        let end = end_arg
            .to_int()
            .ok_or_else(|| "Expected integer values for ZRANGE start and end".to_string())?;

        Ok(ZRangeCommand { key, start, end })
    }
}

impl ZRangeCommand {
    pub fn treat_bounds(&mut self, set_size: i64) -> Option<(usize, usize)> {
        if self.start >= set_size {
            return None;
        }

        if self.start < 0 {
            self.start += set_size
        }

        let start = self.start.max(0) as usize;

        if self.end < 0 {
            self.end += set_size
        }

        if self.end > set_size {
            self.end = set_size - 1
        };

        let end = self.end.max(0) as usize;
        if self.start > self.end {
            return None;
        }
        Some((start, end))
    }
}
