#![allow(unused_imports)]
mod commands;
mod types;

use std::{collections::HashMap, ops::ControlFlow, sync::Arc};

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
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let _stream = listener.accept().await;
        match _stream {
            Ok((stream, _)) => {
                println!("accepted new connection");
                tokio::spawn(async move {
                    client_process(stream).await;
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn client_process(mut stream: TcpStream) {
    let mut buf = [0; 512];

    let pairs: Arc<Mutex<HashMap<String, RedisKeyValue>>> = Arc::new(Mutex::new(HashMap::new()));
    let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));

    loop {
        match stream.read(&mut buf).await {
            Ok(0) => return,
            Ok(n) => {
                let request = buf[0..n].to_vec();
                let received_commands = RedisCommand::parse(request);

                for command in received_commands {
                    match command {
                        RedisCommand::GET(key) => match &key {
                            RedisType::BulkString(_) => {
                                match pairs.lock().await.get(&key.to_string()) {
                                    Some(val) => {
                                        if val.is_valid() {
                                            write_stream(
                                                &mut stream,
                                                &RedisType::bulk_string(val.value()),
                                            )
                                            .await;
                                        } else {
                                            write_stream(&mut stream, &RedisType::Null).await;
                                        }
                                    }
                                    None => {
                                        write_stream(&mut stream, &RedisType::Null).await;
                                    }
                                }
                            }
                            _ => (),
                        },
                        RedisCommand::SET(key, value) => {
                            pairs.lock().await.insert(key.to_string(), value);
                            write_stream(&mut stream, &RedisType::ok()).await;
                        }
                        RedisCommand::PING => {
                            write_stream(&mut stream, &RedisType::pong()).await;
                        }
                        RedisCommand::ECHO(value) => {
                            write_stream(&mut stream, &value).await;
                        },
                        RedisCommand::RPUSH(key, values) => match &key {
                            RedisType::BulkString(_) => {
                                let key_str = key.to_string();
                                match lists.lock().await.get_mut(&key_str) {
                                    Some(list_values) => {
                                        list_values.extend_from_slice(values.as_slice());
                                        let len: i64 = list_values.len().try_into().expect("Error to convert list length");
                                        write_stream(&mut stream, &RedisType::Integer(len)).await;
                                    }
                                    None => {
                                        lists.lock().await.insert(key_str, values).expect("cant add to the hashmap");
                                        write_stream(&mut stream, &RedisType::Integer(1)).await;
                                    }
                                }
                            }
                            _ => (),
                        }
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

async fn write_stream(stream: &mut TcpStream, value: &RedisType) {
    if stream.write_all(&value.serialize()).await.is_err() {
        println!("error writing in stream");
    }
}
