/// Representa um valor no Redis Serialization Protocol (RESP).
/// `#[derive(Debug, PartialEq)]` nos permite imprimir para depuração e comparar valores nos testes.
use crate::types::RedisType;

#[derive(Debug, PartialEq)]
pub enum RedisCommand {
    Get(RedisType),
    Set(RedisType, RedisType),
    Ping,
    Echo(RedisType),
}

impl RedisCommand {
    pub fn build(values: Vec<RedisType>) -> Vec<RedisCommand> {
        let mut commands: Vec<RedisCommand> = Vec::new();

        for value in values {
            match value {
                RedisType::Array(arr) => {
                    // https://redis.io/docs/latest/develop/reference/protocol-spec/#sending-commands-to-a-redis-server
                    // o client até pode mandar outros tipos, mas o que o protocolo entende como entrada de comandos
                    // é somente um array de bulk strings
                    if arr.is_empty() {
                        continue;
                    }
                    let mut args = arr.into_iter();
                    if let Some(RedisType::BulkString(command_bytes)) = args.next() {
                        if let Ok(command_name) = String::from_utf8(command_bytes) {
                            match command_name.to_ascii_uppercase().as_str() {
                                "PING" => {
                                    commands.push(RedisCommand::Ping);
                                }
                                "ECHO" => {
                                    if let Some(s) = args.next() {
                                        commands.push(RedisCommand::Echo(s));
                                    }
                                }
                                "GET" => {
                                    if let Some(s) = args.next() {
                                        commands.push(RedisCommand::Get(s));
                                    }
                                }
                                "SET" => {
                                    if let (Some(key), Some(value)) = (args.next(), args.next()) {
                                        commands.push(RedisCommand::Set(key, value));
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
                RedisType::BulkString(bytes) if bytes.eq_ignore_ascii_case(b"PING") => {
                    commands.push(RedisCommand::Ping);
                }
                RedisType::SimpleString(s) if s.eq_ignore_ascii_case("PING") => {
                    commands.push(RedisCommand::Ping);
                }
                _ => (),
            }
        }
        commands
    }

    pub fn parse(values: Vec<u8>) -> Vec<RedisCommand>{
        let received_values = RedisType::parse(values);
        println!("Received values: {:?}", received_values);

        let received_commands = Self::build(received_values);
        println!("Received commands: {:?}", received_commands);
        
        return received_commands;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commands_build_ping() {
        let result = RedisCommand::build(vec![
            RedisType::bulk_string("ping"),
            RedisType::simple_string("ping"),
        ]);
        assert_eq!(result, vec![RedisCommand::Ping, RedisCommand::Ping]);
    }

    #[test]
    fn test_commands_build_echo() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["echo", "teste teste"])]);
        assert_eq!(
            result,
            vec![RedisCommand::Echo(RedisType::bulk_string("teste teste"))]
        );
    }

    #[test]
    fn test_commands_build_get() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["get", "test"])]);
        assert_eq!(
            result,
            vec![RedisCommand::Get(RedisType::bulk_string("test"))]
        );
    }

    #[test]
    fn test_commands_build_set() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["set", "test", "test"])]);
        assert_eq!(
            result,
            vec![RedisCommand::Set(
                RedisType::bulk_string("test"),
                RedisType::bulk_string("test")
            )]
        );
    }
}
