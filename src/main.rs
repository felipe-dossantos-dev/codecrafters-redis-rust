#![allow(unused_imports)]
mod protocol;

use tokio::{
    io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, Result},
    net::{TcpListener, TcpStream},
};

use crate::protocol::RespValue;

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
                let resps = RespValue::parse(request);
                println!("Received: {:?}", resps);
                if resps[0] == RespValue::BulkString(String::from("PING").into_bytes()) {
                    if stream.write_all(&RespValue::BulkString(String::from("PONG").into_bytes()).serialize()).await.is_err() {
                        return;
                    }
                } else if resps[0] == RespValue::BulkString(String::from("ECHO").into_bytes()) {
                    if stream.write_all(&resps[1].serialize()).await.is_err() {
                        return;
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
