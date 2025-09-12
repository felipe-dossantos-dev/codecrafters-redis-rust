use std::{
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        HashMap, HashSet, VecDeque,
    },
    future::Future,
    hash::Hash,
    io,
    sync::Arc,
};

use nanoid::nanoid;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, Result},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Notify},
    time::Instant,
};

use crate::{
    commands::{RedisCommand, RedisKeyValue},
    types::RedisType,
    utils,
};

#[derive(Debug)]
struct RedisStore {
    pub pairs: Mutex<HashMap<String, RedisKeyValue>>,
    pub lists: Mutex<HashMap<String, VecDeque<String>>>,
    // para cada estrutura de dados com a key "X", tem vários clientes esperando ser notificados por algo
    pub client_notifiers: Mutex<HashMap<String, Vec<Arc<Notify>>>>,
}

impl RedisStore {
    pub fn new() -> Self {
        Self {
            pairs: Mutex::new(HashMap::new()),
            lists: Mutex::new(HashMap::new()),
            client_notifiers: Mutex::new(HashMap::new()),
        }
    }
}

#[derive(Debug)]
pub struct RedisClient<T> {
    pub id: String,
    pub created_at: Instant,
    pub notifier: Arc<Notify>,
    pub tcp_stream: T,
}

impl<T> PartialEq for RedisClient<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.created_at == other.created_at
    }
}

impl<T> Eq for RedisClient<T> {}

impl<T> Hash for RedisClient<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.created_at.hash(state);
    }
}

impl<T> RedisClient<T> {
    pub fn new(tcp_stream: T) -> Self
    where
        T: AsyncReadExt + AsyncWriteExt + Unpin + Send,
    {
        return Self {
            id: nanoid!(),
            created_at: Instant::now(),
            notifier: Arc::new(Notify::new()),
            tcp_stream,
        };
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
                    let received_commands = RedisCommand::parse(request);

                    for command in received_commands {
                        let response = Self::handle_command(command, &client, &store).await;
                        Self::write_stream(&mut client.tcp_stream, &response).await;
                        println!("Response Generated: {:?}", response);
                    }
                }
                Err(e) => {
                    println!("error: {}", e);
                    return;
                }
            }
        }
    }

    // TODO - essa interface está ruim
    // pq ela está fazendo muita coisa, como processar o comando e atualizar outros clientes sobre isso
    // atualiza a store tbm... 
    // e ela está ruim pq já mudei a interface dela várias vezes
    async fn handle_command(
        command: RedisCommand,
        client: &RedisClient<impl AsyncReadExt + AsyncWriteExt + Unpin + Send>,
        store: &Arc<RedisStore>,
    ) -> Option<RedisType>
    where
    {
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
                let key_clone = key.clone();
                let list = lists_guard.entry(key).or_insert_with(VecDeque::new);
                if list.is_empty() {
                    if let Some(clients_notifiers) = store.client_notifiers.lock().await.get(&key_clone) {
                        for client_notifier in clients_notifiers {
                            client_notifier.notify_waiters();
                        }
                    }
                }
                list.extend(values);
                let len = list.len() as i64;
                Some(RedisType::Integer(len))
            }
            RedisCommand::LPUSH(key, values) => {
                let mut lists_guard = store.lists.lock().await;
                let key_clone = key.clone();
                let list = lists_guard.entry(key).or_insert_with(VecDeque::new);
                if list.is_empty() {
                    if let Some(clients_notifiers) = store.client_notifiers.lock().await.get(&key_clone) {
                        for client_notifier in clients_notifiers {
                            client_notifier.notify_waiters();
                        }
                    }
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
                        return Some(RedisType::Null);
                    }

                    let client_notifier_clone = Arc::clone(&client.notifier);

                    let mut notifiers = store.client_notifiers.lock().await;
                    notifiers
                        .entry(key.clone())
                        .or_insert_with(|| Vec::new())
                        .push(client_notifier_clone);

                    if timeout > 0.0 {
                        let remaining_timeout = std::time::Duration::from_millis(
                            (timeout * 1000.0) as u64 - elapsed as u64,
                        );
                        tokio::select! {
                            _ = client.notifier.notified() => {},
                            _ = tokio::time::sleep(remaining_timeout) => return Some(RedisType::Null),
                        }
                    } else {
                        client.notifier.notified().await;
                    }

                    if let Some(vec) = notifiers.get_mut(&key) {
                        vec.retain(|n| !Arc::ptr_eq(n, &client.notifier));
                    }
                }
            }
        }
    }

    async fn write_stream<T>(stream: &mut T, value: &Option<RedisType>)
    where
        T: AsyncWriteExt + Unpin,
    {
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
    use std::collections::HashMap;
    use std::sync::Arc;

    use tokio::io::DuplexStream;
    use tokio::sync::Mutex;

    use super::*;
    use crate::commands::RedisCommand;
    use crate::types::RedisType;

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
}
