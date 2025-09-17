use std::{
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        HashMap, VecDeque,
    },
    sync::Arc,
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

use crate::{
    client::RedisClient,
    commands::{RedisCommand, RedisKeyValue},
    store::{RedisStore, WaitResult},
    types::RedisType,
    utils,
};

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
                    let client: RedisClient<TcpStream> = RedisClient::new(stream);
                    tokio::spawn(async move {
                        Self::client_process(client, store_clone).await;
                    });
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
    }

    async fn client_process<T>(mut client: RedisClient<T>, store: Arc<RedisStore>)
    where
        T: AsyncReadExt + AsyncWriteExt + Unpin + Send,
    {
        let mut buf = [0; 512];

        loop {
            match client.tcp_stream.read(&mut buf).await {
                Ok(0) => return,
                Ok(n) => {
                    let request = buf[0..n].to_vec();
                    let parsed_commands = RedisCommand::parse(request);

                    match parsed_commands {
                        Ok(received_commands) => {
                            for command in received_commands {
                                let response = Self::handle_command(command, &client, &store).await;
                                client.write_response(&response).await;
                                println!(
                                    "Response Generated for client:{:?} {:?}",
                                    client.id, response
                                );
                            }
                        }
                        Err(msg) => {
                            client
                                .write_response(&Some(RedisType::Error(msg.clone())))
                                .await;
                            println!("Response Generated for client:{:?} {}", client.id, msg);
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
        client: &RedisClient<impl AsyncReadExt + AsyncWriteExt + Unpin + Send>,
        store: &Arc<RedisStore>,
    ) -> Option<RedisType> {
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
                let list = lists_guard.entry(key.clone()).or_insert_with(VecDeque::new);
                if list.is_empty() {
                    store.notify_by_key(&key).await;
                }
                list.extend(values);
                let len = list.len() as i64;

                Some(RedisType::Integer(len))
            }
            RedisCommand::LPUSH(key, values) => {
                let mut lists_guard = store.lists.lock().await;
                let list = lists_guard.entry(key.clone()).or_insert_with(VecDeque::new);
                if list.is_empty() {
                    store.notify_by_key(&key).await;
                }
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
                let start_time = utils::now_millis();
                loop {
                    let lpop_result = lpop(&store.lists, key.clone(), 1).await;
                    if !matches!(lpop_result, Some(RedisType::Null)) {
                        return lpop_result.map(|val| {
                            RedisType::Array(vec![RedisType::bulk_string(key.as_str()), val])
                        });
                    }

                    let elapsed = utils::now_millis() - start_time;
                    if timeout > 0.0 && elapsed >= (timeout * 1000.0) as u128 {
                        return Some(RedisType::NullArray);
                    }

                    let remaining_timeout = std::time::Duration::from_millis(
                        (timeout * 1000.0) as u64 - elapsed as u64,
                    );
                    match store
                        .wait_until_timeout(&key, remaining_timeout, &client.notifier)
                        .await
                    {
                        WaitResult::Timeout => {
                            return Some(RedisType::NullArray);
                        }
                        _ => {}
                    };
                }
            }
            RedisCommand::ZADD(key, _, values) => {
                let mut ss_guard = store.sorted_sets.lock().await;
                let ss = ss_guard.entry(key.clone()).or_insert_with(VecDeque::new);
                let len = values.len() as i64;
                ss.extend(values);
                Some(RedisType::Integer(len))
            },
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
    use std::collections::HashMap;
    use std::sync::Arc;

    use tokio::io::DuplexStream;
    use tokio::sync::Mutex;

    use super::*;
    use crate::commands::RedisCommand;
    use crate::commands::RedisKeyValue;
    use crate::commands::SortedAddOptions;
    use crate::commands::SortedValue;
    use crate::types::RedisType;
    use crate::utils;

    fn new_client_for_test() -> (RedisClient<DuplexStream>, DuplexStream) {
        let (client_stream, server_stream) = tokio::io::duplex(1024);
        (RedisClient::new(client_stream), server_stream)
    }

    #[tokio::test]
    async fn test_handle_rpush_new_list() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let command = RedisCommand::RPUSH(
            "mylist".to_string(),
            vec!["one".to_string(), "two".to_string()],
        );

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RedisType::Integer(2));
        let lists_guard = store.lists.lock().await;
        assert_eq!(
            lists_guard.get("mylist"),
            Some(&VecDeque::from(vec!["one".to_string(), "two".to_string()]))
        );
    }

    #[tokio::test]
    async fn test_handle_lpush_new_list() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let command = RedisCommand::LPUSH(
            "mylist".to_string(),
            vec!["one".to_string(), "two".to_string()],
        );

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RedisType::Integer(2));
        let lists_guard = store.lists.lock().await;
        assert_eq!(
            lists_guard.get("mylist"),
            Some(&VecDeque::from(["two".to_string(), "one".to_string()]))
        );
    }

    #[tokio::test]
    async fn test_handle_rpush_existing_list() {
        let (client, _server_stream) = new_client_for_test();
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

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

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
        let (client, _server_stream) = new_client_for_test();
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

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

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
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let command = RedisCommand::LRANGE("no-such-list".to_string(), 0, 1);

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RedisType::Array(vec![]));
    }

    #[tokio::test]
    async fn test_handle_lrange_empty_list() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        store
            .lists
            .lock()
            .await
            .insert("mylist".to_string(), VecDeque::new());

        let command = RedisCommand::LRANGE("mylist".to_string(), 0, 1);

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RedisType::Array(vec![]));
    }

    #[tokio::test]
    async fn test_handle_lrange_full_range() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        store.lists.lock().await.insert(
            "mylist".to_string(),
            VecDeque::from(["one".to_string(), "two".to_string(), "three".to_string()]),
        );

        let command = RedisCommand::LRANGE("mylist".to_string(), 0, 2);

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

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
        let (client, _server_stream) = new_client_for_test();
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

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(
            result,
            RedisType::Array(vec![
                RedisType::bulk_string("d"),
                RedisType::bulk_string("e"),
            ])
        );

        let command = RedisCommand::LRANGE("mylist".to_string(), 0, -3);

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(
            result,
            RedisType::Array(vec![
                RedisType::bulk_string("a"),
                RedisType::bulk_string("b"),
                RedisType::bulk_string("c"),
            ])
        );

        let command = RedisCommand::LRANGE("mylist".to_string(), -6, -1);

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

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
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        store
            .lists
            .lock()
            .await
            .insert("mylist".to_string(), VecDeque::from(["one".to_string()]));

        let command = RedisCommand::LRANGE("mylist".to_string(), 0, 10);

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(
            result,
            RedisType::Array(vec![RedisType::bulk_string("one")])
        );
    }

    #[tokio::test]
    async fn test_handle_lrange_start_out_of_bounds() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        store
            .lists
            .lock()
            .await
            .insert("mylist".to_string(), VecDeque::from(["one".to_string()]));

        let command = RedisCommand::LRANGE("mylist".to_string(), 5, 10);

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RedisType::Array(vec![]));
    }

    #[tokio::test]
    async fn test_handle_llen() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        store
            .lists
            .lock()
            .await
            .insert("mylist".to_string(), VecDeque::from(["one".to_string()]));

        let command = RedisCommand::LLEN("mylist".to_string());

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RedisType::Integer(1));
    }

    #[tokio::test]
    async fn test_handle_lpop() {
        let (client, _server_stream) = new_client_for_test();
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
        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();
        assert_eq!(result, RedisType::bulk_string("1"));
        let lists_guard = store.lists.lock().await;
        assert_eq!(
            lists_guard.get("mylist").unwrap(),
            &VecDeque::from(["2".to_string(), "3".to_string(), "4".to_string()])
        );
    }

    #[tokio::test]
    async fn test_handle_lpop_multiple() {
        let (client, _server_stream) = new_client_for_test();
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

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

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

    #[tokio::test]
    async fn test_handle_ping() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let command = RedisCommand::PING;

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RedisType::pong());
    }

    #[tokio::test]
    async fn test_handle_echo() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let command = RedisCommand::ECHO("hello world".to_string());

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RedisType::bulk_string("hello world"));
    }

    #[tokio::test]
    async fn test_handle_get_non_existent_key_in_server() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let get_command = RedisCommand::GET("nonexistent".to_string());
        let get_result = RedisServer::handle_command(get_command, &client, &store)
            .await
            .unwrap();
        assert_eq!(get_result, RedisType::Null);
    }

    #[tokio::test]
    async fn test_handle_get_existing_key() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let key = "mykey".to_string();
        let value = RedisKeyValue {
            value: "myvalue".to_string(),
            expired_at_millis: None,
        };
        store.pairs.lock().await.insert(key.clone(), value);

        let command = RedisCommand::GET(key);
        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RedisType::bulk_string("myvalue"));
    }

    #[tokio::test]
    async fn test_handle_get_expired_key() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let key = "mykey".to_string();
        let value = RedisKeyValue {
            value: "myvalue".to_string(),
            expired_at_millis: Some(utils::now_millis() - 1),
        };
        store.pairs.lock().await.insert(key.clone(), value);

        let command = RedisCommand::GET(key);
        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RedisType::Null);
    }

    #[tokio::test]
    async fn test_handle_set() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let key = "mykey".to_string();
        let value = RedisKeyValue {
            value: "myvalue".to_string(),
            expired_at_millis: None,
        };

        let command = RedisCommand::SET(key.clone(), value);
        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RedisType::ok());
        let pairs_guard = store.pairs.lock().await;
        let stored_value = pairs_guard.get(&key).unwrap();
        assert_eq!(stored_value.value, "myvalue");
        assert!(stored_value.expired_at_millis.is_none());
    }

    #[tokio::test]
    async fn test_handle_set_with_px() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let key = "mykey".to_string();
        let value = RedisKeyValue {
            value: "myvalue".to_string(),
            expired_at_millis: Some(utils::now_millis() + 10000),
        };

        let command = RedisCommand::SET(key.clone(), value);
        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RedisType::ok());
        let pairs_guard = store.pairs.lock().await;
        let stored_value = pairs_guard.get(&key).unwrap();
        assert_eq!(stored_value.value, "myvalue");
        assert!(stored_value.expired_at_millis.is_some()); // Access public field
    }

    #[tokio::test]
    async fn test_lpop_non_existent_key() {
        let lists = Mutex::new(HashMap::new());
        let result = lpop(&lists, "no-such-key".to_string(), 1).await;
        assert_eq!(result, Some(RedisType::Null));
    }

    #[tokio::test]
    async fn test_lpop_empty_list() {
        let lists = Mutex::new(HashMap::from([("empty-list".to_string(), VecDeque::new())]));
        let result = lpop(&lists, "empty-list".to_string(), 1).await;
        assert_eq!(result, Some(RedisType::Null));
    }

    #[tokio::test]
    async fn test_lpop_more_than_exists() {
        let lists = Mutex::new(HashMap::from([(
            "mylist".to_string(),
            VecDeque::from(["one".to_string()]),
        )]));
        let result = lpop(&lists, "mylist".to_string(), 2).await;
        assert_eq!(
            result,
            Some(RedisType::Array(vec![RedisType::bulk_string("one")]))
        );
        let lists_guard = lists.lock().await;
        assert!(lists_guard.get("mylist").unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_handle_blpop_item_exists() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        store
            .lists
            .lock()
            .await
            .insert("myblist".to_string(), VecDeque::from(["one".to_string()]));

        let command = RedisCommand::BLPOP("myblist".to_string(), 0.0);
        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(
            result,
            RedisType::Array(vec![
                RedisType::bulk_string("myblist"),
                RedisType::bulk_string("one")
            ])
        );
    }

    #[tokio::test]
    async fn test_handle_blpop_timeout() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let timeout_secs = 0.1; // Timeout curto para o teste

        let command = RedisCommand::BLPOP("myblist".to_string(), timeout_secs);
        let start = tokio::time::Instant::now();
        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();
        let duration = start.elapsed();

        assert_eq!(result, RedisType::NullArray);
        assert!(duration >= Duration::from_secs_f64(timeout_secs));
    }

    #[tokio::test]
    async fn test_handle_blpop_waits_for_rpush() {
        // 1. Setup: Store compartilhado e um cliente para o BLPOP
        let store = Arc::new(RedisStore::new());
        let (blpop_client, _blpop_stream) = new_client_for_test();
        let key = "myblist_wait".to_string();

        // 2. Task 1: Executa o BLPOP em uma nova task.
        // Ele ficará bloqueado pois a lista 'myblist_wait' está vazia.
        let store_for_blpop = store.clone();
        let key_for_blpop = key.clone();
        let blpop_handle = tokio::spawn(async move {
            let command = RedisCommand::BLPOP(key_for_blpop, 2.0); // Timeout de 2s
            RedisServer::handle_command(command, &blpop_client, &store_for_blpop).await
        });

        // 3. Pausa breve para garantir que o BLPOP já começou a esperar.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // 4. Task 2: Outro cliente faz um RPUSH, que deve notificar e desbloquear o BLPOP.
        let (rpush_client, _rpush_stream) = new_client_for_test();
        let rpush_command = RedisCommand::RPUSH(key.clone(), vec!["value1".to_string()]);
        RedisServer::handle_command(rpush_command, &rpush_client, &store).await;

        // 5. Aguarda o resultado da task do BLPOP e verifica se está correto.
        let blpop_result = blpop_handle.await.unwrap().unwrap();

        assert_eq!(
            blpop_result,
            RedisType::Array(vec![
                RedisType::bulk_string(&key),
                RedisType::bulk_string("value1")
            ])
        );
    }

    #[tokio::test]
    async fn test_handle_zadd() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());

        let command = RedisCommand::ZADD(
            "mylist".to_string(),
            SortedAddOptions::new(),
            vec![
                SortedValue {
                    member: "1".to_string(),
                    score: 0.1,
                },
                SortedValue {
                    member: "2".to_string(),
                    score: 1.0,
                },
            ],
        );

        let result = RedisServer::handle_command(command, &client, &store).await;

        assert_eq!(result, Some(RedisType::Integer(2)));
    }
}
