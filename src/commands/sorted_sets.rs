use std::vec::IntoIter;

use crate::types::RedisType;

#[derive(Debug, PartialEq, Clone)]
pub struct SortedAddOptions {
    pub xx: bool,   // Only update elements that already exist. Don't add new elements.
    pub nx: bool,   // Only add new elements. Don't update already existing elements.
    pub lt: bool, // Only update existing elements if the new score is less than the current score. This flag doesn't prevent adding new elements.
    pub gt: bool, // Only update existing elements if the new score is greater than the current score. This flag doesn't prevent adding new elements.
    pub ch: bool, // Modify the return value from the number of new elements added, to the total number of elements changed.
    pub incr: bool, // When this option is specified ZADD acts like ZINCRBY. Only one score-element pair can be specified in this mode.
}

impl SortedAddOptions {
    pub fn new() -> SortedAddOptions {
        return SortedAddOptions {
            xx: false,
            nx: false,
            lt: false,
            gt: false,
            ch: false,
            incr: false,
        };
    }

    pub fn parse(args: &mut IntoIter<RedisType>) -> Self {
        let mut options = SortedAddOptions::new();

        while let Some(prop_name) = args.as_slice().get(0) {
            let option_str = prop_name
                .to_string()
                .expect("cannot convert value to string")
                .to_ascii_uppercase();
            match option_str.as_str() {
                "XX" => {
                    options.xx = true;
                }
                "NX" => {
                    options.nx = true;
                }
                "LT" => {
                    options.lt = true;
                }
                "GT" => {
                    options.gt = true;
                }
                "CH" => {
                    options.ch = true;
                }
                "INCR" => {
                    options.incr = true;
                }
                _ => {
                    break;
                }
            }
            args.next();
        }
        options
    }
}



#[derive(Debug, PartialEq, Clone)]
pub struct SortedValue {
    pub member: String,
    pub score: f64,
}



impl SortedValue {
    pub fn parse(args: &mut IntoIter<RedisType>) -> Option<Vec<SortedValue>> {
        let mut sorted_values: Vec<SortedValue> = Vec::new();
        while let (Some(score_arg), Some(member_arg)) = (args.next(), args.next()) {
            if let (Some(score), Some(member)) = (score_arg.to_float(), member_arg.to_string()) {
                sorted_values.push(SortedValue { member, score });
            } else {
                return None;
            }
        }
        if sorted_values.is_empty() {
            None
        } else {
            Some(sorted_values)
        }
    }
}
