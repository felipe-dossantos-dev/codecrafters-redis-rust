use std::{
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        HashMap, VecDeque,
    },
    future::Future,
    io,
    sync::Arc,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, Result},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

use crate::{
    commands::{RedisCommand, RedisKeyValue},
    types::RedisType,
    utils,
};

#[derive(Debug, PartialEq)]
pub struct RedisRequest {
    command: RedisCommand,
    expired_at_millis: Option<u128>,
}

impl RedisRequest {
    pub fn is_expired(&self) -> bool {
        if let Some(val) = self.expired_at_millis {
            return utils::now_millis() > val;
        }
        false
    }

    pub fn new(command: RedisCommand) -> Self {
        match command {
            RedisCommand::BLPOP(_, timeout) => {
                if timeout > 0.0 {
                    return Self {
                        command: command,
                        expired_at_millis: Some(utils::now_millis() + (timeout * 1000.0) as u128),
                    };
                } else {
                    return Self {
                        command: command,
                        expired_at_millis: None,
                    };
                }
            }
            _ => {
                return Self {
                    command,
                    expired_at_millis: None,
                };
            }
        }
    }
}

#[derive(Debug)]
struct RedisStore {
    pub pairs: Mutex<HashMap<String, RedisKeyValue>>,
    pub lists: Mutex<HashMap<String, VecDeque<String>>>,
    pub reques: Mutex<VecDeque<RedisRequest>>,
}

impl RedisStore {
    pub fn new() -> Self {
        Self {
            pairs: Mutex::new(HashMap::new()),
            lists: Mutex::new(HashMap::new()),
            reques: Mutex::new(VecDeque::new()),
        }
    }
}

#[derive(Debug)]
pub struct RedisServer {
    addr: String,
    store: Arc<RedisStore>,
}

impl RedisServer {
    pub fn new(addr: String) -> Self {
        Self {
            addr,
            store: Arc::new(RedisStore::new()),
        }
    }

    pub async fn run(&self) {
        let listener = match TcpListener::bind(self.addr.clone()).await {
            Ok(listener) => listener,
            Err(e) => {
                eprintln!("Failed to bind to address {}: {}", self.addr, e);
                return;
            }
        };
        loop {
            let stream = listener.accept().await;
            match stream {
                Ok((stream, _)) => {
                    println!("accepted new connection");
                    let store_clone = Arc::clone(&self.store);
                    tokio::spawn(async move {
                        Self::client_process(stream, store_clone).await;
                    });
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
    }

    async fn client_process(mut stream: TcpStream, store: Arc<RedisStore>) {
        let mut buf = [0; 512];

        loop {
            match stream.read(&mut buf).await {
                Ok(0) => return,
                Ok(n) => {
                    let request = buf[0..n].to_vec();
                    let received_commands = RedisCommand::parse(request);

                    for command in received_commands {
                        let response = Self::handle_command(command, &store).await;
                        Self::write_stream(&mut stream, &response).await;
                        println!("Response Generated: {:?}", response);
                    }
                }
                Err(e) => {
                    println!("error: {}", e);
                    return;
                }
            }

            loop {
                let mut reques = store.reques.lock().await;
                if reques.is_empty() {
                    break;
                }

                let request = reques.pop_front().unwrap();
                if !request.is_expired() {
                    println!("reques request: {:?}", request);
                    let response = Self::handle_command(request.command, &store).await;
                    Self::write_stream(&mut stream, &response).await;
                }
            }
        }
    }

    async fn handle_command(command: RedisCommand, store: &Arc<RedisStore>) -> Option<RedisType> {
        match command {
            RedisCommand::GET(key) => {
                let response = match store.pairs.lock().await.get(&key.to_string()) {
                    Some(val) if !val.is_expired() => RedisType::bulk_string(&val.value),
                    _ => RedisType::Null,
                };
                Some(response)
            }
            RedisCommand::SET(key, value) => {
                store.pairs.lock().await.insert(key.to_string(), value);
                Some(RedisType::ok())
            }
            RedisCommand::PING => Some(RedisType::pong()),
            RedisCommand::ECHO(value) => Some(RedisType::bulk_string(&value)),
            RedisCommand::RPUSH(key, values) => {
                let mut lists_guard = store.lists.lock().await;
                let list = lists_guard.entry(key).or_insert_with(VecDeque::new);
                list.extend(values);
                let len = list.len() as i64;
                Some(RedisType::Integer(len))
            }
            RedisCommand::LPUSH(key, values) => {
                let mut lists_guard = store.lists.lock().await;
                let list = lists_guard.entry(key).or_insert_with(VecDeque::new);
                for value in values.iter() {
                    list.insert(0, value.clone());
                }
                let len = list.len() as i64;
                Some(RedisType::Integer(len))
            }
            RedisCommand::LRANGE(key, mut start_index, mut end_index) => {
                if let Some(list_value) = store.lists.lock().await.get(&key.to_string()) {
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
            RedisCommand::LLEN(key) => {
                let mut lists_guard = store.lists.lock().await;
                let list = lists_guard.entry(key).or_insert_with(VecDeque::new);
                let len = list.len() as i64;
                Some(RedisType::Integer(len))
            }
            RedisCommand::LPOP(key, count) => lpop(&store.lists, key, count).await,
            RedisCommand::BLPOP(key, timeout) => {
                // TODO - criar uma classe request
                // TODO - refatorar a parte dos comandos separadamente e tbm para não ficar passando as listas para lá e para cá
                //      - cria algo com o nome de store
                let key_clone = key.clone();
                let lpop = lpop(&store.lists, key, 1).await;
                match lpop {
                    Some(value) => match value {
                        RedisType::Null => {
                            store.reques.lock().await.push_back(RedisRequest::new(
                                RedisCommand::BLPOP(key_clone, timeout),
                            ));
                            None
                        }
                        val => Some(val),
                    },
                    None => None,
                }
            }
        }
    }

    async fn write_stream(stream: &mut TcpStream, value: &Option<RedisType>) {
        if let Some(val) = value {
            if stream.write_all(&val.serialize()).await.is_err() {
                eprintln!("error writing to stream");
            }
        }
    }
}

async fn lpop(
    lists: &Mutex<HashMap<String, VecDeque<String>>>,
    key: String,
    count: i64,
) -> Option<RedisType> {
    let mut lists_guard = lists.lock().await;
    let list = lists_guard.entry(key);
    match list {
        Occupied(mut occupied_entry) => {
            let mut popped_elements: Vec<RedisType> = Vec::new();
            for _i in 0..count {
                if let Some(val) = occupied_entry.get_mut().pop_front() {
                    popped_elements.push(RedisType::bulk_string(val.as_str()));
                } else {
                    break;
                }
            }
            if count == 1 && !popped_elements.is_empty() {
                return Some(popped_elements.remove(0));
            } else if count > 1 {
                return Some(RedisType::Array(popped_elements));
            }
            Some(RedisType::Null)
        }
        Vacant(_) => Some(RedisType::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::RedisCommand;
    use crate::types::RedisType;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_handle_rpush_new_list() {
        let store = Arc::new(RedisStore::new());
        let command = RedisCommand::RPUSH(
            "mylist".to_string(),
            vec!["one".to_string(), "two".to_string()],
        );

        let result = RedisServer::handle_command(command, &store).await.unwrap();

        assert_eq!(result, RedisType::Integer(2));
        let lists_guard = store.lists.lock().await;
        assert_eq!(
            lists_guard.get("mylist"),
            Some(&VecDeque::from(vec!["one".to_string(), "two".to_string()]))
        );
    }

    #[tokio::test]
    async fn test_handle_lpush_new_list() {
        let store = Arc::new(RedisStore::new());
        let command = RedisCommand::LPUSH(
            "mylist".to_string(),
            vec!["one".to_string(), "two".to_string()],
        );

        let result = RedisServer::handle_command(command, &store).await.unwrap();

        assert_eq!(result, RedisType::Integer(2));
        let lists_guard = store.lists.lock().await;
        assert_eq!(
            lists_guard.get("mylist"),
            Some(&VecDeque::from(["two".to_string(), "one".to_string()]))
        );
    }

    #[tokio::test]
    async fn test_handle_rpush_existing_list() {
        let store = Arc::new(RedisStore::new());
        store
            .lists
            .lock()
            .await
            .insert("mylist".to_string(), VecDeque::from(["zero".to_string()]));

        let command = RedisCommand::RPUSH(
            "mylist".to_string(),
            vec!["one".to_string(), "two".to_string()],
        );

        let result = RedisServer::handle_command(command, &store).await.unwrap();

        assert_eq!(result, RedisType::Integer(3));
        let lists_guard = store.lists.lock().await;
        assert_eq!(
            lists_guard.get("mylist"),
            Some(&VecDeque::from([
                "zero".to_string(),
                "one".to_string(),
                "two".to_string()
            ]))
        );
    }

    #[tokio::test]
    async fn test_handle_lpush_existing_list() {
        let store = Arc::new(RedisStore::new());
        store
            .lists
            .lock()
            .await
            .insert("mylist".to_string(), VecDeque::from(["zero".to_string()]));

        let command = RedisCommand::LPUSH(
            "mylist".to_string(),
            vec!["one".to_string(), "two".to_string()],
        );

        let result = RedisServer::handle_command(command, &store).await.unwrap();

        assert_eq!(result, RedisType::Integer(3));
        let lists_guard = store.lists.lock().await;
        assert_eq!(
            lists_guard.get("mylist"),
            Some(&VecDeque::from([
                "two".to_string(),
                "one".to_string(),
                "zero".to_string(),
            ]))
        );
    }

    #[tokio::test]
    async fn test_handle_lrange_non_existent_key() {
        let store = Arc::new(RedisStore::new());
        let command = RedisCommand::LRANGE("no-such-list".to_string(), 0, 1);

        let result = RedisServer::handle_command(command, &store).await.unwrap();

        assert_eq!(result, RedisType::Array(vec![]));
    }

    #[tokio::test]
    async fn test_handle_lrange_empty_list() {
        let store = Arc::new(RedisStore::new());
        store
            .lists
            .lock()
            .await
            .insert("mylist".to_string(), VecDeque::new());

        let command = RedisCommand::LRANGE("mylist".to_string(), 0, 1);

        let result = RedisServer::handle_command(command, &store).await.unwrap();

        assert_eq!(result, RedisType::Array(vec![]));
    }

    #[tokio::test]
    async fn test_handle_lrange_full_range() {
        let store = Arc::new(RedisStore::new());
        store.lists.lock().await.insert(
            "mylist".to_string(),
            VecDeque::from(["one".to_string(), "two".to_string(), "three".to_string()]),
        );

        let command = RedisCommand::LRANGE("mylist".to_string(), 0, 2);

        let result = RedisServer::handle_command(command, &store).await.unwrap();

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
        let store = Arc::new(RedisStore::new());
        store.lists.lock().await.insert(
            "mylist".to_string(),
            VecDeque::from([
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
                "e".to_string(),
            ]),
        );
        let command = RedisCommand::LRANGE("mylist".to_string(), -2, -1);

        let result = RedisServer::handle_command(command, &store).await.unwrap();

        assert_eq!(
            result,
            RedisType::Array(vec![
                RedisType::bulk_string("d"),
                RedisType::bulk_string("e"),
            ])
        );

        let command = RedisCommand::LRANGE("mylist".to_string(), 0, -3);

        let result = RedisServer::handle_command(command, &store).await.unwrap();

        assert_eq!(
            result,
            RedisType::Array(vec![
                RedisType::bulk_string("a"),
                RedisType::bulk_string("b"),
                RedisType::bulk_string("c"),
            ])
        );

        let command = RedisCommand::LRANGE("mylist".to_string(), -6, -1);

        let result = RedisServer::handle_command(command, &store).await.unwrap();

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
        let store = Arc::new(RedisStore::new());
        store
            .lists
            .lock()
            .await
            .insert("mylist".to_string(), VecDeque::from(["one".to_string()]));

        let command = RedisCommand::LRANGE("mylist".to_string(), 0, 10);

        let result = RedisServer::handle_command(command, &store).await.unwrap();

        assert_eq!(
            result,
            RedisType::Array(vec![RedisType::bulk_string("one")])
        );
    }

    #[tokio::test]
    async fn test_handle_lrange_start_out_of_bounds() {
        let store = Arc::new(RedisStore::new());
        store
            .lists
            .lock()
            .await
            .insert("mylist".to_string(), VecDeque::from(["one".to_string()]));

        let command = RedisCommand::LRANGE("mylist".to_string(), 5, 10);

        let result = RedisServer::handle_command(command, &store).await.unwrap();

        assert_eq!(result, RedisType::Array(vec![]));
    }

    #[tokio::test]
    async fn test_handle_llen() {
        let store = Arc::new(RedisStore::new());
        store
            .lists
            .lock()
            .await
            .insert("mylist".to_string(), VecDeque::from(["one".to_string()]));

        let command = RedisCommand::LLEN("mylist".to_string());

        let result = RedisServer::handle_command(command, &store).await.unwrap();

        assert_eq!(result, RedisType::Integer(1));
    }

    #[tokio::test]
    async fn test_handle_lpop() {
        let store = Arc::new(RedisStore::new());
        store.lists.lock().await.insert(
            "mylist".to_string(),
            VecDeque::from([
                "1".to_string(),
                "2".to_string(),
                "3".to_string(),
                "4".to_string(),
            ]),
        );

        let command = RedisCommand::LPOP("mylist".to_string(), 1);
        let result = RedisServer::handle_command(command, &store).await.unwrap();
        assert_eq!(result, RedisType::bulk_string("1"));
        let lists_guard = store.lists.lock().await;
        assert_eq!(
            lists_guard.get("mylist").unwrap(),
            &VecDeque::from(["2".to_string(), "3".to_string(), "4".to_string()])
        );
    }

    #[tokio::test]
    async fn test_handle_lpop_multiple() {
        let store = Arc::new(RedisStore::new());
        store.lists.lock().await.insert(
            "mylist".to_string(),
            VecDeque::from([
                "1".to_string(),
                "2".to_string(),
                "3".to_string(),
                "4".to_string(),
            ]),
        );
        let command = RedisCommand::LPOP("mylist".to_string(), 2);

        let result = RedisServer::handle_command(command, &store).await.unwrap();

        assert_eq!(
            result,
            RedisType::Array(vec![
                RedisType::bulk_string("1"),
                RedisType::bulk_string("2")
            ])
        );
        let lists_guard = store.lists.lock().await;
        assert_eq!(
            lists_guard.get("mylist").unwrap(),
            &VecDeque::from(["3".to_string(), "4".to_string()])
        );
    }
}
