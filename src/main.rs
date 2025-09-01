#![allow(unused_imports)]
mod commands;
mod types;

use std::{collections::HashMap, ops::ControlFlow, sync::Arc};

use tokio::{
    io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, Result},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

use crate::{commands::RedisCommand, types::RedisType};

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

    loop {
        match stream.read(&mut buf).await {
            Ok(0) => return,
            Ok(n) => {
                let request = buf[0..n].to_vec();
                let received_commands = RedisCommand::parse(request);

                let pairs: Arc<Mutex<HashMap<String, String>>> =
                    Arc::new(Mutex::new(HashMap::new()));

                for command in received_commands {
                    match command {
                        RedisCommand::Get(key) => match key {
                            RedisType::BulkString(s) => {
                                let key_string = String::from_utf8(s).unwrap();
                                match pairs.lock().await.get(key_string.as_str()) {
                                    Some(val) => {
                                        write_stream(&mut stream, &RedisType::bulk_string(val)).await;
                                    }
                                    None => {
                                        write_stream(&mut stream, &RedisType::Null).await;
                                    }
                                }
                            }
                            _ => (),
                        },
                        RedisCommand::Set(key, value) => match (&key, &value) {
                            (RedisType::BulkString(_), RedisType::BulkString(_))  => {                                
                                pairs.lock().await.insert(key.to_string(), value.to_string());
                                write_stream(&mut stream, &RedisType::ok()).await;
                            }
                            _ => (),
                        },
                        RedisCommand::Ping => {
                            write_stream(&mut stream, &RedisType::pong()).await;
                        }
                        RedisCommand::Echo(value) => {
                            write_stream(&mut stream, &value).await;
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

async fn write_stream(stream: &mut TcpStream, value: &RedisType){
    if stream
        .write_all(
            &value.serialize()
        )
        .await
        .is_err()
    {
        println!("error writing in stream");
    }
}
