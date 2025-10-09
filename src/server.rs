use std::{
    collections::{
        hash_map::Entry::{Occupied, Vacant},
        BTreeSet, HashMap, VecDeque,
    },
    sync::Arc,
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Notify},
};

use crate::{
    client::RedisClient,
    commands::{traits::RunnableCommand, zadd::ZAddCommand, RedisCommand},
    values::RedisValue,
    resp::RespDataType,
    store::{RedisStore, WaitResult},
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

    async fn client_process<T: AsyncRead + AsyncWrite + Unpin + Send>(
        mut client: RedisClient<T>,
        store: Arc<RedisStore>,
    ) {
        loop {
            match client.connection.read_request().await {
                Ok(None) => break,
                Ok(Some(request)) => {
                    let parsed_commands = RedisCommand::parse(request);

                    match parsed_commands {
                        Ok(received_commands) => {
                            for command in received_commands {
                                let response = Self::handle_command(command, &client, &store).await;
                                client.connection.write_response(&response).await;
                                println!(
                                    "Response Generated for client:{:?} {:?}",
                                    client.id, response
                                );
                            }
                        }
                        Err(msg) => {
                            client
                                .connection
                                .write_response(&Some(RespDataType::Error(msg.clone())))
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
    ) -> Option<RespDataType> {
        command.execute(&client.id, store, &client.notifier).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use tokio::io::DuplexStream;
    use tokio::sync::Mutex;

    use super::*;
    use crate::commands::key_type::KeyTypeCommand;
    use crate::commands::lpop;
    use crate::commands::zadd::ZAddOptions;
    use crate::commands::{
        blpop::BLPopCommand, echo::EchoCommand, get::GetCommand, llen::LLenCommand,
        lpop::LPopCommand, lpush::LPushCommand, lrange::LRangeCommand, ping::PingCommand,
        rpush::RPushCommand, set::SetCommand, zadd::ZAddCommand, zcard::ZCardCommand,
        zrange::ZRangeCommand, zrank::ZRankCommand, zrem::ZRemCommand, zscore::ZScoreCommand,
    };
    use crate::values::key_value::KeyValue;
    use crate::values::sorted_set::SortedValue;
    use crate::resp::RespDataType;
    use crate::utils;

    fn new_client_for_test() -> (RedisClient<DuplexStream>, DuplexStream) {
        let (server_stream, client_stream) = tokio::io::duplex(1024);
        (RedisClient::mock_new(server_stream), client_stream)
    }

    #[tokio::test]
    async fn test_handle_rpush_new_list() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let command = RedisCommand::RPUSH(RPushCommand {
            key: "mylist".to_string(),
            values: vec!["one".to_string(), "two".to_string()],
        });

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RespDataType::Integer(2));
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
        let command = RedisCommand::LPUSH(LPushCommand {
            key: "mylist".to_string(),
            values: vec!["one".to_string(), "two".to_string()],
        });

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RespDataType::Integer(2));
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

        let command = RedisCommand::RPUSH(RPushCommand {
            key: "mylist".to_string(),
            values: vec!["one".to_string(), "two".to_string()],
        });

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RespDataType::Integer(3));
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

        let command = RedisCommand::LPUSH(LPushCommand {
            key: "mylist".to_string(),
            values: vec!["one".to_string(), "two".to_string()],
        });

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RespDataType::Integer(3));
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
        let command = RedisCommand::LRANGE(LRangeCommand {
            key: "no-such-list".to_string(),
            start: 0,
            end: 1,
        });

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RespDataType::Array(vec![]));
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

        let command = RedisCommand::LRANGE(LRangeCommand {
            key: "mylist".to_string(),
            start: 0,
            end: 1,
        });

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RespDataType::Array(vec![]));
    }

    #[tokio::test]
    async fn test_handle_lrange_full_range() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        store.lists.lock().await.insert(
            "mylist".to_string(),
            VecDeque::from(["one".to_string(), "two".to_string(), "three".to_string()]),
        );

        let command = RedisCommand::LRANGE(LRangeCommand {
            key: "mylist".to_string(),
            start: 0,
            end: 2,
        });

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(
            result,
            RespDataType::Array(vec![
                RespDataType::bulk_string("one"),
                RespDataType::bulk_string("two"),
                RespDataType::bulk_string("three"),
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
        let command = RedisCommand::LRANGE(LRangeCommand {
            key: "mylist".to_string(),
            start: -2,
            end: -1,
        });

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(
            result,
            RespDataType::Array(vec![
                RespDataType::bulk_string("d"),
                RespDataType::bulk_string("e"),
            ])
        );

        let command = RedisCommand::LRANGE(LRangeCommand {
            key: "mylist".to_string(),
            start: 0,
            end: -3,
        });

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(
            result,
            RespDataType::Array(vec![
                RespDataType::bulk_string("a"),
                RespDataType::bulk_string("b"),
                RespDataType::bulk_string("c"),
            ])
        );

        let command = RedisCommand::LRANGE(LRangeCommand {
            key: "mylist".to_string(),
            start: -6,
            end: -1,
        });

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(
            result,
            RespDataType::Array(vec![
                RespDataType::bulk_string("a"),
                RespDataType::bulk_string("b"),
                RespDataType::bulk_string("c"),
                RespDataType::bulk_string("d"),
                RespDataType::bulk_string("e"),
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

        let command = RedisCommand::LRANGE(LRangeCommand {
            key: "mylist".to_string(),
            start: 0,
            end: 10,
        });

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(
            result,
            RespDataType::Array(vec![RespDataType::bulk_string("one")])
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

        let command = RedisCommand::LRANGE(LRangeCommand {
            key: "mylist".to_string(),
            start: 5,
            end: 10,
        });

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RespDataType::Array(vec![]));
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

        let command = RedisCommand::LLEN(LLenCommand {
            key: "mylist".to_string(),
        });

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RespDataType::Integer(1));
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

        let command = RedisCommand::LPOP(LPopCommand {
            key: "mylist".to_string(),
            count: 1,
        });
        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();
        assert_eq!(result, RespDataType::bulk_string("1"));
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
        let command = RedisCommand::LPOP(LPopCommand {
            key: "mylist".to_string(),
            count: 2,
        });

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(
            result,
            RespDataType::Array(vec![
                RespDataType::bulk_string("1"),
                RespDataType::bulk_string("2")
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
        let command = RedisCommand::PING(PingCommand);

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RespDataType::pong());
    }

    #[tokio::test]
    async fn test_handle_echo() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let command = RedisCommand::ECHO(EchoCommand {
            message: "hello world".to_string(),
        });

        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RespDataType::bulk_string("hello world"));
    }

    #[tokio::test]
    async fn test_handle_get_non_existent_key_in_server() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let get_command = RedisCommand::GET(GetCommand {
            key: "nonexistent".to_string(),
        });
        let get_result = RedisServer::handle_command(get_command, &client, &store)
            .await
            .unwrap();
        assert_eq!(get_result, RespDataType::Null);
    }

    #[tokio::test]
    async fn test_handle_get_existing_key() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let key = "mykey".to_string();
        let value = KeyValue {
            value: "myvalue".to_string(),
            expired_at_millis: None,
        };
        store.pairs.lock().await.insert(key.clone(), value);

        let command = RedisCommand::GET(GetCommand {
            key: key.to_string(),
        });
        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RespDataType::bulk_string("myvalue"));
    }

    #[tokio::test]
    async fn test_handle_get_expired_key() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let key = "mykey".to_string();
        let value = KeyValue {
            value: "myvalue".to_string(),
            expired_at_millis: Some(utils::now_millis() - 1),
        };
        store.pairs.lock().await.insert(key.clone(), value);

        let command = RedisCommand::GET(GetCommand {
            key: key.to_string(),
        });
        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RespDataType::Null);
    }

    #[tokio::test]
    async fn test_handle_set() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let key = "mykey".to_string();
        let value = KeyValue {
            value: "myvalue".to_string(),
            expired_at_millis: None,
        };

        let command = RedisCommand::SET(SetCommand {
            key: key.clone(),
            value,
        });
        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RespDataType::ok());
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
        let value = KeyValue {
            value: "myvalue".to_string(),
            expired_at_millis: Some(utils::now_millis() + 10000),
        };

        let command = RedisCommand::SET(SetCommand {
            key: key.clone(),
            value,
        });
        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(result, RespDataType::ok());
        let pairs_guard = store.pairs.lock().await;
        let stored_value = pairs_guard.get(&key).unwrap();
        assert_eq!(stored_value.value, "myvalue");
        assert!(stored_value.expired_at_millis.is_some()); // Access public field
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

        let command = RedisCommand::BLPOP(BLPopCommand {
            key: "myblist".to_string(),
            timeout: 0.0,
        });
        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        assert_eq!(
            result,
            RespDataType::Array(vec![
                RespDataType::bulk_string("myblist"),
                RespDataType::bulk_string("one")
            ])
        );
    }

    #[tokio::test]
    async fn test_handle_blpop_timeout() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let timeout_secs = 0.1; // Timeout curto para o teste

        let command = RedisCommand::BLPOP(BLPopCommand {
            key: "myblist".to_string(),
            timeout: timeout_secs,
        });
        let start = tokio::time::Instant::now();
        let result = RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();
        let duration = start.elapsed();

        assert_eq!(result, RespDataType::NullArray);
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
            let command = RedisCommand::BLPOP(BLPopCommand {
                key: key_for_blpop,
                timeout: 2.0,
            }); // Timeout de 2s
            RedisServer::handle_command(command, &blpop_client, &store_for_blpop).await
        });

        // 3. Pausa breve para garantir que o BLPOP já começou a esperar.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // 4. Task 2: Outro cliente faz um RPUSH, que deve notificar e desbloquear o BLPOP.
        let (rpush_client, _rpush_stream) = new_client_for_test();
        let rpush_command = RedisCommand::RPUSH(RPushCommand {
            key: key.clone(),
            values: vec!["value1".to_string()],
        });
        RedisServer::handle_command(rpush_command, &rpush_client, &store).await;

        // 5. Aguarda o resultado da task do BLPOP e verifica se está correto.
        let blpop_result = blpop_handle.await.unwrap().unwrap();

        assert_eq!(
            blpop_result,
            RespDataType::Array(vec![
                RespDataType::bulk_string(&key),
                RespDataType::bulk_string("value1")
            ])
        );
    }

    #[tokio::test]
    async fn test_handle_zadd() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());

        let command = RedisCommand::ZADD(ZAddCommand {
            key: "mylist".to_string(),
            options: ZAddOptions::new(),
            values: vec![
                SortedValue {
                    member: "1".to_string(),
                    score: 0.1,
                },
                SortedValue {
                    member: "2".to_string(),
                    score: 1.0,
                },
                SortedValue {
                    member: "2".to_string(),
                    score: 2.0,
                },
            ],
        });

        let result = RedisServer::handle_command(command, &client, &store).await;

        assert_eq!(result, Some(RespDataType::Integer(2)));
    }

    #[tokio::test]
    async fn test_handle_zrank() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());

        let command = RedisCommand::ZADD(ZAddCommand {
            key: "zset_key".to_string(),
            options: ZAddOptions::new(),
            values: vec![
                SortedValue {
                    score: 100.0,
                    member: "foo".to_string(),
                },
                SortedValue {
                    score: 100.0,
                    member: "bar".to_string(),
                },
                SortedValue {
                    score: 20.0,
                    member: "baz".to_string(),
                },
                SortedValue {
                    score: 30.1,
                    member: "caz".to_string(),
                },
                SortedValue {
                    score: 40.2,
                    member: "paz".to_string(),
                },
            ],
        });

        RedisServer::handle_command(command, &client, &store).await;

        let command = RedisCommand::ZRANK(ZRankCommand {
            key: "other_key".to_string(),
            member: "caz".to_string(),
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(result, Some(RespDataType::Null));

        let command = RedisCommand::ZRANK(ZRankCommand {
            key: "zset_key".to_string(),
            member: "caz".to_string(),
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(result, Some(RespDataType::Integer(1)));

        let command = RedisCommand::ZRANK(ZRankCommand {
            key: "zset_key".to_string(),
            member: "baz".to_string(),
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(result, Some(RespDataType::Integer(0)));

        let command = RedisCommand::ZRANK(ZRankCommand {
            key: "zset_key".to_string(),
            member: "foo".to_string(),
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(result, Some(RespDataType::Integer(4)));

        let command = RedisCommand::ZRANK(ZRankCommand {
            key: "zset_key".to_string(),
            member: "bar".to_string(),
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(result, Some(RespDataType::Integer(3)));
    }

    #[tokio::test]
    async fn test_handle_zrange() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());

        let command: RedisCommand = RedisCommand::ZADD(ZAddCommand {
            key: "zset_key".to_string(),
            options: ZAddOptions::new(),
            values: vec![
                SortedValue {
                    score: 100.0,
                    member: "foo".to_string(),
                },
                SortedValue {
                    score: 100.0,
                    member: "bar".to_string(),
                },
                SortedValue {
                    score: 20.0,
                    member: "baz".to_string(),
                },
                SortedValue {
                    score: 30.1,
                    member: "caz".to_string(),
                },
                SortedValue {
                    score: 40.2,
                    member: "paz".to_string(),
                },
            ],
        });

        RedisServer::handle_command(command, &client, &store).await;

        let command = RedisCommand::ZRANGE(ZRangeCommand {
            key: "other_key".to_string(),
            start: 0,
            end: 1,
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(result, Some(RespDataType::Array(vec![])));

        let command = RedisCommand::ZRANGE(ZRangeCommand {
            key: "zset_key".to_string(),
            start: 10,
            end: 11,
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(result, Some(RespDataType::Array(vec![])));

        let command = RedisCommand::ZRANGE(ZRangeCommand {
            key: "zset_key".to_string(),
            start: 4,
            end: 2,
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(result, Some(RespDataType::Array(vec![])));

        let command = RedisCommand::ZRANGE(ZRangeCommand {
            key: "zset_key".to_string(),
            start: 0,
            end: 10,
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(
            result,
            Some(RespDataType::Array(vec![
                RespDataType::bulk_string("baz"),
                RespDataType::bulk_string("caz"),
                RespDataType::bulk_string("paz"),
                RespDataType::bulk_string("bar"),
                RespDataType::bulk_string("foo"),
            ]))
        );

        let command = RedisCommand::ZRANGE(ZRangeCommand {
            key: "zset_key".to_string(),
            start: 2,
            end: 4,
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(
            result,
            Some(RespDataType::Array(vec![
                RespDataType::bulk_string("paz"),
                RespDataType::bulk_string("bar"),
                RespDataType::bulk_string("foo"),
            ]))
        );
    }

    #[tokio::test]
    async fn test_handle_zcard() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());

        let command: RedisCommand = RedisCommand::ZADD(ZAddCommand {
            key: "zset_key".to_string(),
            options: ZAddOptions::new(),
            values: vec![
                SortedValue {
                    score: 100.0,
                    member: "foo".to_string(),
                },
                SortedValue {
                    score: 100.0,
                    member: "bar".to_string(),
                },
                SortedValue {
                    score: 20.0,
                    member: "baz".to_string(),
                },
                SortedValue {
                    score: 30.1,
                    member: "caz".to_string(),
                },
                SortedValue {
                    score: 40.2,
                    member: "paz".to_string(),
                },
            ],
        });

        RedisServer::handle_command(command, &client, &store).await;

        let command = RedisCommand::ZCARD(ZCardCommand {
            key: "other_key".to_string(),
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(result, Some(RespDataType::Integer(0)));

        let command = RedisCommand::ZCARD(ZCardCommand {
            key: "zset_key".to_string(),
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(result, Some(RespDataType::Integer(5)));
    }

    #[tokio::test]
    async fn test_handle_zscore() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());

        let command: RedisCommand = RedisCommand::ZADD(ZAddCommand {
            key: "zset_key".to_string(),
            options: ZAddOptions::new(),
            values: vec![
                SortedValue {
                    score: 100.0,
                    member: "foo".to_string(),
                },
                SortedValue {
                    score: 100.0,
                    member: "bar".to_string(),
                },
                SortedValue {
                    score: 20.0,
                    member: "baz".to_string(),
                },
                SortedValue {
                    score: 30.1,
                    member: "caz".to_string(),
                },
                SortedValue {
                    score: 40.2,
                    member: "paz".to_string(),
                },
            ],
        });

        RedisServer::handle_command(command, &client, &store).await;

        let command = RedisCommand::ZSCORE(ZScoreCommand {
            key: "other_key".to_string(),
            member: "foo".to_string(),
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(result, Some(RespDataType::Null));

        let command = RedisCommand::ZSCORE(ZScoreCommand {
            key: "zset_key".to_string(),
            member: "other".to_string(),
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(result, Some(RespDataType::Null));

        let command = RedisCommand::ZSCORE(ZScoreCommand {
            key: "zset_key".to_string(),
            member: "paz".to_string(),
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(result, Some(RespDataType::bulk_string("40.2")));
    }

    #[tokio::test]
    async fn test_handle_zrem() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());

        let command: RedisCommand = RedisCommand::ZADD(ZAddCommand {
            key: "zset_key".to_string(),
            options: ZAddOptions::new(),
            values: vec![
                SortedValue {
                    score: 100.0,
                    member: "foo".to_string(),
                },
                SortedValue {
                    score: 100.0,
                    member: "bar".to_string(),
                },
            ],
        });

        RedisServer::handle_command(command, &client, &store).await;

        let command = RedisCommand::ZREM(ZRemCommand {
            key: "other_key".to_string(),
            member: "foo".to_string(),
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(result, Some(RespDataType::Integer(0)));

        let command = RedisCommand::ZREM(ZRemCommand {
            key: "zset_key".to_string(),
            member: "other".to_string(),
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(result, Some(RespDataType::Integer(0)));

        let command = RedisCommand::ZREM(ZRemCommand {
            key: "zset_key".to_string(),
            member: "foo".to_string(),
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(result, Some(RespDataType::Integer(1)));
    }

    #[tokio::test]
    async fn test_handle_type() {
        let (client, _server_stream) = new_client_for_test();
        let store = Arc::new(RedisStore::new());
        let command = RedisCommand::SET(SetCommand {
            key: "key".to_string(),
            value: KeyValue {
                value: "Value".to_string(),
                expired_at_millis: None,
            },
        });

        RedisServer::handle_command(command, &client, &store)
            .await
            .unwrap();

        let command = RedisCommand::TYPE(KeyTypeCommand {
            key: "zset_key".to_string(),
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(result, Some(RespDataType::simple_string("none")));

        let command = RedisCommand::TYPE(KeyTypeCommand {
            key: "key".to_string(),
        });
        let result = RedisServer::handle_command(command, &client, &store).await;
        assert_eq!(result, Some(RespDataType::simple_string("string")));
    }
}
