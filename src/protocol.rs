// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
// significa que é um array com 2 elementos
// $4 significa que é uma string de tamanho 4
// $3 significa que é uma string de tamanho 3

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

// 1. A struct e sua implementação
// struct Person {
//     name: String,
//     age: u32,
// }

// impl Person {
//     fn new(name: String, age: u32) -> Self {
//         Self { name, age }
//     }

//     // Nota: Testar funções que imprimem na tela (com side-effects)
//     // é mais complexo. Para testes unitários, focamos em funções
//     // que retornam valores ou mudam o estado da struct.
//     fn greet(&self) {
//         println!(
//             "Olá, meu nome é {} e eu tenho {} anos.",
//             self.name, self.age
//         );
//     }
// }


/// Representa um valor no Redis Serialization Protocol (RESP).
/// `#[derive(Debug, PartialEq)]` nos permite imprimir para depuração e comparar valores nos testes.
#[derive(Debug, PartialEq)]
pub enum RespValue {
    /// Representa strings simples. Começa com `+`. Ex: `+OK\r\n`
    SimpleString(String),

    /// Representa erros. Começa com `-`. Ex: `-Error message\r\n`
    Error(String),

    /// Representa inteiros. Começa com `:`. Ex: `:1000\r\n`
    Integer(i64),

    /// Representa strings binariamente seguras. Começa com `$`. Ex: `$6\r\nfoobar\r\n`
    BulkString(Vec<u8>),

    /// Representa uma lista de outros tipos RESP. Começa com `*`. Ex: `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`
    Array(Vec<RespValue>),

    /// Representa a ausência de valor (null). Em RESP2, é codificado como `_\r\n` (Null Bulk String).
    Null,
}

impl RespValue {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            RespValue::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            _ => String::from("").into_bytes()
        }
    }
}

// criar a struct / impl

// 2. O módulo de testes
// A anotação `#[cfg(test)]` diz ao compilador para só incluir
// este código quando executamos `cargo test`.
#[cfg(test)]
mod tests {
    // `use super::*;` importa tudo do módulo pai (neste caso, Person).
    use super::*;

    // A anotação `#[test]` marca esta função como um teste.
    
    fn test_array_creation() {
        let arr = RespValue::Array(vec![
            RespValue::BulkString(b"ECHO".to_vec()),
            RespValue::BulkString(b"hey".to_vec()),
        ]);

        let expected = RespValue::Array(vec![
            RespValue::BulkString(vec![69, 67, 72, 79]), // "ECHO" em bytes
            RespValue::BulkString(vec![104, 101, 121]), // "hey" em bytes
        ]);

        assert_eq!(arr, expected);
    }

    // #[test]
    // fn test_new_person_initializes_correctly() {
    //     // Arrange: Preparamos os dados para o teste.
    //     let name = String::from("Carlos");
    //     let age = 45;

    //     // Act: Executamos a função que queremos testar.
    //     let person = Person::new(name.clone(), age);

    //     // Assert: Verificamos se o resultado é o esperado.
    //     // A macro `assert_eq!` falha o teste se os dois lados não forem iguais.
    //     assert_eq!(person.name, "Carlos");
    //     assert_eq!(person.age, 45);
    // }
}
