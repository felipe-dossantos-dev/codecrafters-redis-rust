use std::
    vec::IntoIter
;

use crate::{resp::RespDataType, utils};

#[derive(Debug, PartialEq, Clone)]
pub struct RedisKeyValue {
    pub value: String,
    pub expired_at_millis: Option<u128>,
}

impl RedisKeyValue {
    pub fn is_expired(&self) -> bool {
        if let Some(val) = self.expired_at_millis {
            return val <= utils::now_millis();
        }
        false
    }

    pub fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let value = args
            .next()
            .ok_or_else(|| "Expected a value for RedisKeyValue".to_string())?;

        let mut expired_at_millis: Option<u128> = None;

        while let Some(prop_name) = args.next().and_then(|p| p.to_string()) {
            match prop_name.to_ascii_uppercase().as_str() {
                "PX" => {
                    let ttl_arg = args
                        .next()
                        .and_then(|p| p.to_string())
                        .ok_or("PX option requires a value")?;
                    let ttl_value: u128 = ttl_arg
                        .parse()
                        .map_err(|e| format!("Cannot parse PX value: {}", e))?;
                    expired_at_millis = Some(utils::now_millis() + ttl_value);
                }
                _ => (),
            }
        }
        let value_str = value
            .to_string()
            .ok_or("Expected a string value for RedisKeyValue")?;
        Ok(Self {
            value: value_str,
            expired_at_millis,
        })
    }
}


