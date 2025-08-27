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
                match &resps[0] {
                    RespValue::BulkString(s) => {
                        if *s == b"PING".to_vec() {
                            println!("ping command");
                            if stream.write_all(&RespValue::SimpleString(String::from("PONG")).serialize()).await.is_err() {
                                return;
                            }
                        }
                    }
                    RespValue::Array(ref arr) => {
                        if arr.len() > 0 {
                            if let RespValue::BulkString(s) = &arr[0] {
                                if s.to_ascii_uppercase() == b"ECHO".to_vec() {
                                    if arr.len() > 1 {
                                        if stream.write_all(&arr[1].serialize()).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    RespValue::SimpleString(_) => todo!(),
                    RespValue::Error(_) => todo!(),
                    RespValue::Integer(_) => todo!(),
                    RespValue::Null => todo!(),
                }
            }
            Err(e) => {
                println!("error: {}", e);
                return;
            }
        }
    }
}
