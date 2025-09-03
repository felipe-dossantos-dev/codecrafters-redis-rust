#![allow(unused_imports)]
mod commands;
mod types;
use std::{collections::HashMap, sync::Arc};

use tokio::{
    io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, Result},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

use crate::{
    commands::{RedisCommand, RedisKeyValue},
    types::RedisType,
};

#[tokio::main]
async fn main() -> Result<()> {
    // TODO: transformar isso numa classe de RedisServer e já facilitar os testes nessa classe
    // manter o mecanisco thread safe de acesso das estruturas de dados
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let pairs: Arc<Mutex<HashMap<String, RedisKeyValue>>> = Arc::new(Mutex::new(HashMap::new()));
    let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let _stream = listener.accept().await;
        match _stream {
            Ok((stream, _)) => {
                println!("accepted new connection");
                let pairs_clone = pairs.clone();
                let lists_clone = lists.clone();
                tokio::spawn(async move {
                    client_process(stream, pairs_clone, lists_clone).await;
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn client_process(
    mut stream: TcpStream,
    pairs: Arc<Mutex<HashMap<String, RedisKeyValue>>>,
    lists: Arc<Mutex<HashMap<String, Vec<String>>>>,
) {
    let mut buf = [0; 512];

    loop {
        match stream.read(&mut buf).await {
            Ok(0) => return,
            Ok(n) => {
                let request = buf[0..n].to_vec();
                let received_commands = RedisCommand::parse(request);

                for command in received_commands {
                    if let Some(response) = handle_command(command, &pairs, &lists).await {
                        print!("Response Generated: {:?}", response);
                        write_stream(&mut stream, &response).await;
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
                return;
            }
        }
    }
}

async fn handle_command(
    command: RedisCommand,
    pairs: &Arc<Mutex<HashMap<String, RedisKeyValue>>>,
    lists: &Arc<Mutex<HashMap<String, Vec<String>>>>,
) -> Option<RedisType> {
    match command {
        // TODO: mover esses match do RedisType para os commands, não faz sentido essa validação aqui
        // o comando já deve vir com os tipos certos senão não deve ser construido
        // e dar erro quando não conseguir construir um comando válido
        RedisCommand::GET(key) => match &key {
            RedisType::BulkString(_) => {
                let response = match pairs.lock().await.get(&key.to_string()) {
                    Some(val) if !val.is_expired() => RedisType::bulk_string(val.value()),
                    _ => RedisType::Null,
                };
                Some(response)
            }
            _ => None,
        },
        RedisCommand::SET(key, value) => {
            pairs.lock().await.insert(key.to_string(), value);
            Some(RedisType::ok())
        }
        RedisCommand::PING => Some(RedisType::pong()),
        RedisCommand::ECHO(value) => Some(value),
        RedisCommand::RPUSH(key, values) => match &key {
            RedisType::BulkString(_) => {
                let key_str = key.to_string();
                let mut lists_guard = lists.lock().await;
                let list = lists_guard.entry(key_str).or_insert_with(Vec::new);
                list.extend(values);
                let len = list.len() as i64;
                Some(RedisType::Integer(len))
            }
            _ => None,
        },
        RedisCommand::LRANGE(key, start, end) => match (&key, &start, &end) {
            (
                RedisType::BulkString(_),
                RedisType::Integer(start_index),
                RedisType::Integer(end_index),
            ) => {
                if let Some(list_value) = lists.lock().await.get(&key.to_string()) {
                    let list_len = list_value.len() as i64;
                    let final_end_index = if end_index > &list_len { &(list_len - 1) } else { end_index };
                    let mut result_list: Vec<RedisType> = Vec::new();

                    // TODO: essas validações deveria ser na construção da struct (fica mais facil de testar tbm)
                    if start_index < &list_len && start_index <= final_end_index {
                        for i in *start_index..=*final_end_index {
                            result_list.push(RedisType::bulk_string(&list_value[i as usize]));
                        }
                        return Some(RedisType::Array(result_list));
                    }
                }
                Some(RedisType::Array(vec![]))
            }
            _ => None,
        },
    }
}

async fn write_stream(stream: &mut TcpStream, value: &RedisType) {
    if stream.write_all(&value.serialize()).await.is_err() {
        println!("error writing in stream");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_handle_rpush_new_list() {
        let lists = Arc::new(Mutex::new(HashMap::new()));
        let pairs = Arc::new(Mutex::new(HashMap::new()));
        let command = RedisCommand::RPUSH(
            RedisType::bulk_string("mylist"),
            vec!["one".to_string(), "two".to_string()],
        );

        let result = handle_command(command, &pairs, &lists).await.unwrap();

        assert_eq!(result, RedisType::Integer(2));
        let lists_guard = lists.lock().await;
        assert_eq!(
            lists_guard.get("mylist"),
            Some(&vec!["one".to_string(), "two".to_string()])
        );
    }

    #[tokio::test]
    async fn test_handle_rpush_existing_list() {
        let mut initial_list = HashMap::new();
        initial_list.insert("mylist".to_string(), vec!["zero".to_string()]);
        let lists = Arc::new(Mutex::new(initial_list));
        let pairs = Arc::new(Mutex::new(HashMap::new()));

        let command = RedisCommand::RPUSH(
            RedisType::bulk_string("mylist"),
            vec!["one".to_string(), "two".to_string()],
        );

        let result = handle_command(command, &pairs, &lists).await.unwrap();

        assert_eq!(result, RedisType::Integer(3));
        let lists_guard = lists.lock().await;
        assert_eq!(
            lists_guard.get("mylist"),
            Some(&vec![
                "zero".to_string(),
                "one".to_string(),
                "two".to_string()
            ])
        );
    }

    #[tokio::test]
    async fn test_handle_lrange_non_existent_key() {
        let lists = Arc::new(Mutex::new(HashMap::new()));
        let pairs = Arc::new(Mutex::new(HashMap::new()));
        let command = RedisCommand::LRANGE(
            RedisType::bulk_string("no-such-list"),
            RedisType::Integer(0),
            RedisType::Integer(1),
        );

        let result = handle_command(command, &pairs, &lists).await.unwrap();

        assert_eq!(result, RedisType::Array(vec![]));
    }

    #[tokio::test]
    async fn test_handle_lrange_empty_list() {
        let mut initial_list = HashMap::new();
        initial_list.insert("mylist".to_string(), vec![]);
        let lists = Arc::new(Mutex::new(initial_list));
        let pairs = Arc::new(Mutex::new(HashMap::new()));

        let command = RedisCommand::LRANGE(
            RedisType::bulk_string("mylist"),
            RedisType::Integer(0),
            RedisType::Integer(1),
        );

        let result = handle_command(command, &pairs, &lists).await.unwrap();

        assert_eq!(result, RedisType::Array(vec![]));
    }

    #[tokio::test]
    async fn test_handle_lrange_full_range() {
        let mut initial_list = HashMap::new();
        initial_list.insert(
            "mylist".to_string(),
            vec!["one".to_string(), "two".to_string(), "three".to_string()],
        );
        let lists = Arc::new(Mutex::new(initial_list));
        let pairs = Arc::new(Mutex::new(HashMap::new()));

        let command = RedisCommand::LRANGE(
            RedisType::bulk_string("mylist"),
            RedisType::Integer(0),
            RedisType::Integer(2),
        );

        let result = handle_command(command, &pairs, &lists).await.unwrap();

        assert_eq!(
            result,
            RedisType::Array(vec![
                RedisType::bulk_string("one"),
                RedisType::bulk_string("two"),
                RedisType::bulk_string("three"),
            ])
        );
    }

    #[tokio::test]
    async fn test_handle_lrange_end_out_of_bounds() {
        let mut initial_list = HashMap::new();
        initial_list.insert("mylist".to_string(), vec!["one".to_string()]);
        let lists = Arc::new(Mutex::new(initial_list));
        let pairs = Arc::new(Mutex::new(HashMap::new()));

        let command = RedisCommand::LRANGE(
            RedisType::bulk_string("mylist"),
            RedisType::Integer(0),
            RedisType::Integer(10),
        );

        let result = handle_command(command, &pairs, &lists).await.unwrap();

        assert_eq!(result, RedisType::Array(vec![RedisType::bulk_string("one")]));
    }

    #[tokio::test]
    async fn test_handle_lrange_start_out_of_bounds() {
        let mut initial_list = HashMap::new();
        initial_list.insert("mylist".to_string(), vec!["one".to_string()]);
        let lists = Arc::new(Mutex::new(initial_list));
        let pairs = Arc::new(Mutex::new(HashMap::new()));

        let command = RedisCommand::LRANGE(
            RedisType::bulk_string("mylist"),
            RedisType::Integer(5),
            RedisType::Integer(10),
        );

        let result = handle_command(command, &pairs, &lists).await.unwrap();

        assert_eq!(result, RedisType::Array(vec![]));
    }
}
