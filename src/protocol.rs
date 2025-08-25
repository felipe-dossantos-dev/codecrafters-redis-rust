// eu tenho que implementar um protocolo minimo
// o que seria esse protocolo minimo? seria uma classe que pega uma buffer e transforma um comando do redis
// e depois essa mesma classe transforma uma comando em um buffer para escrever
// esse seria o modulo para implementar esse protocolo
// como eu organizaria isso em questao de dados e enums
// a IA acabou gerando o RespValue para mim, o que não gostei, mas segue o jogo

// os passos seriam:
// - Terminar o parse e serialize do RespValue (que são os tipos de dados do Redis)
//  - Fazer os testes unitários sozinho para melhorar meu entendimento da linguagem
//  - Caso tiver dúvidas utilizar o gemini para ele não responder com o código que tenho que escrever
// - Criar uma outra estrutura para entender e validar os comandos

use std::str::FromStr;


/// Representa um valor no Redis Serialization Protocol (RESP).
/// `#[derive(Debug, PartialEq)]` nos permite imprimir para depuração e comparar valores nos testes.
#[derive(Debug, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<RespValue>),
    Null,
}

impl RespValue {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            RespValue::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            RespValue::Error(s) => format!("-{}\r\n", s).into_bytes(),
            RespValue::Integer(s) => format!(":{}\r\n", s).into_bytes(),
            RespValue::BulkString(s) => {
                let mut result = format!("${}\r\n", s.len()).into_bytes();
                result.extend_from_slice(s);
                result.extend_from_slice(b"\r\n");
                result
            }
            RespValue::Array(arr) => {
                let mut result = format!("*{}\r\n", arr.len()).into_bytes();
                for elem in arr {
                    result.extend(elem.serialize());    
                }
                result
            }
            _ => format!("_\r\n").into_bytes()
            // TODO - implementar os tests
        }
    }
}

// A anotação `#[cfg(test)]` diz ao compilador para só incluir
// este código quando executamos `cargo test`.
#[cfg(test)]
mod tests {
    // `use super::*;` importa tudo do módulo pai (neste caso, Person).
    use super::*;

    #[test]
    fn test_simple_string_serialization() {
        let result = RespValue::SimpleString(String::from("hey")).serialize();
        let expected = String::from("+hey\r\n").into_bytes();
        assert_eq!(result, expected)
    }

    #[test]
    fn test_error_serialization() {
        let result = RespValue::Error(String::from("hey")).serialize();
        let expected = String::from("-hey\r\n").into_bytes();
        assert_eq!(result, expected)
    }

    #[test]
    fn test_integer_serialization() {
        let result = RespValue::Integer(128).serialize();
        let expected = String::from(":128\r\n").into_bytes();
        assert_eq!(result, expected)
    }

    #[test]
    fn test_bulk_string_serialization() {
        let result = RespValue::BulkString(b"foobar".to_vec()).serialize();
        let expected = b"$6\r\nfoobar\r\n".to_vec();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_empty_bulk_string_serialization() {
        let result = RespValue::BulkString(b"".to_vec()).serialize();
        let expected = b"$0\r\n\r\n".to_vec();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_null_serialization() {
        let result = RespValue::Null.serialize();
        let expected = b"_\r\n".to_vec();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_empty_array_serialization() {
        let result = RespValue::Array(vec![RespValue::Integer(128), RespValue::BulkString(b"foobar".to_vec())]).serialize();
        let expected = b"*2\r\n:128\r\n$6\r\nfoobar\r\n".to_vec();
        assert_eq!(result, expected);
    }


}
