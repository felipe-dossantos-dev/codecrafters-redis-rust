#![allow(unused_imports)]
use tokio::io;
use tokio::{
    io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> io::Result<()> {
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
            Ok(_) => {
                if stream.write_all(b"+PONG\r\n").await.is_err() {
                    return;
                }
            }
            Err(e) => {
                println!("error: {}", e);
                return;
            }
        }
    }
}
