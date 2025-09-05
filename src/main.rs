#![allow(unused_imports)]
mod commands;
mod types;
use std::cmp;
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
                        println!("Response Generated: {:?}", response);
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
        RedisCommand::GET(key) => {
            let response = match pairs.lock().await.get(&key.to_string()) {
                Some(val) if !val.is_expired() => RedisType::bulk_string(&val.value),
                _ => RedisType::Null,
            };
            Some(response)
        }
        RedisCommand::SET(key, value) => {
            pairs.lock().await.insert(key.to_string(), value);
            Some(RedisType::ok())
        }
        RedisCommand::PING => Some(RedisType::pong()),
        RedisCommand::ECHO(value) => Some(RedisType::bulk_string(&value)),
        RedisCommand::RPUSH(key, values) => {
            let key_str = key.to_string();
            let mut lists_guard = lists.lock().await;
            let list = lists_guard.entry(key_str).or_insert_with(Vec::new);
            list.extend(values);
            let len = list.len() as i64;
            Some(RedisType::Integer(len))
        }
        RedisCommand::LRANGE(key, mut start_index, mut end_index) => {
            if let Some(list_value) = lists.lock().await.get(&key.to_string()) {
                let list_len = list_value.len() as i64;

                if start_index > list_len {
                    return Some(RedisType::Array(vec![]));
                }

                if start_index < 0 {
                    start_index += list_len
                }

                let start = start_index.max(0) as usize;

                if end_index < 0 {
                    end_index += list_len
                }

                if end_index > list_len {
                    end_index = list_len - 1
                };

                let end = end_index.max(0) as usize;

                if start_index > end_index {
                    return Some(RedisType::Array(vec![]));
                }

                let mut result_list: Vec<RedisType> = Vec::new();

                for i in start..=end {
                    result_list.push(RedisType::bulk_string(&list_value[i]));
                }
                return Some(RedisType::Array(result_list));
            }
            Some(RedisType::Array(vec![]))
        }
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
            "mylist".to_string(),
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
            "mylist".to_string(),
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
        let command = RedisCommand::LRANGE("no-such-list".to_string(), 0, 1);

        let result = handle_command(command, &pairs, &lists).await.unwrap();

        assert_eq!(result, RedisType::Array(vec![]));
    }

    #[tokio::test]
    async fn test_handle_lrange_empty_list() {
        let mut initial_list = HashMap::new();
        initial_list.insert("mylist".to_string(), vec![]);
        let lists = Arc::new(Mutex::new(initial_list));
        let pairs = Arc::new(Mutex::new(HashMap::new()));

        let command = RedisCommand::LRANGE("mylist".to_string(), 0, 1);

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

        let command = RedisCommand::LRANGE("mylist".to_string(), 0, 2);

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
    async fn test_handle_lrange_negative_range() {
        let mut initial_list = HashMap::new();
        initial_list.insert(
            "mylist".to_string(),
            vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
                "e".to_string(),
            ],
        );
        let lists = Arc::new(Mutex::new(initial_list));
        let pairs = Arc::new(Mutex::new(HashMap::new()));

        let command = RedisCommand::LRANGE("mylist".to_string(), -2, -1);

        let result = handle_command(command, &pairs, &lists).await.unwrap();

        assert_eq!(
            result,
            RedisType::Array(vec![
                RedisType::bulk_string("d"),
                RedisType::bulk_string("e"),
            ])
        );

        let command = RedisCommand::LRANGE("mylist".to_string(), 0, -3);

        let result = handle_command(command, &pairs, &lists).await.unwrap();

        assert_eq!(
            result,
            RedisType::Array(vec![
                RedisType::bulk_string("a"),
                RedisType::bulk_string("b"),
                RedisType::bulk_string("c"),
            ])
        );

        let command = RedisCommand::LRANGE("mylist".to_string(), -6, -1);

        let result = handle_command(command, &pairs, &lists).await.unwrap();

        assert_eq!(
            result,
            RedisType::Array(vec![
                RedisType::bulk_string("a"),
                RedisType::bulk_string("b"),
                RedisType::bulk_string("c"),
                RedisType::bulk_string("d"),
                RedisType::bulk_string("e"),
            ])
        );
    }

    #[tokio::test]
    async fn test_handle_lrange_end_out_of_bounds() {
        let mut initial_list = HashMap::new();
        initial_list.insert("mylist".to_string(), vec!["one".to_string()]);
        let lists = Arc::new(Mutex::new(initial_list));
        let pairs = Arc::new(Mutex::new(HashMap::new()));

        let command = RedisCommand::LRANGE("mylist".to_string(), 0, 10);

        let result = handle_command(command, &pairs, &lists).await.unwrap();

        assert_eq!(
            result,
            RedisType::Array(vec![RedisType::bulk_string("one")])
        );
    }

    #[tokio::test]
    async fn test_handle_lrange_start_out_of_bounds() {
        let mut initial_list = HashMap::new();
        initial_list.insert("mylist".to_string(), vec!["one".to_string()]);
        let lists = Arc::new(Mutex::new(initial_list));
        let pairs = Arc::new(Mutex::new(HashMap::new()));

        let command = RedisCommand::LRANGE("mylist".to_string(), 5, 10);

        let result = handle_command(command, &pairs, &lists).await.unwrap();

        assert_eq!(result, RedisType::Array(vec![]));
    }
}
