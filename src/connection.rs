use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::types::RedisType;

const BUFFER_SIZE: usize = 512;

#[derive(Debug)]
pub struct Connection<T: AsyncRead + AsyncWrite + Unpin + Send> {
    stream: T,
    buffer: [u8; BUFFER_SIZE],
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Connection<T> {
    pub fn new(stream: T) -> Self {
        Self {
            stream,
            buffer: [0; BUFFER_SIZE],
        }
    }

    pub async fn read_request(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        match self.stream.read(&mut self.buffer).await {
            Ok(0) => Ok(None), // Conexão fechada
            Ok(n) => Ok(Some(self.buffer[0..n].to_vec())),
            Err(e) => Err(e),
        }
    }

    pub async fn write_response(&mut self, value: &Option<RedisType>) {
        let response_bytes = match value {
            Some(val) => val.serialize(),
            None => RedisType::Null.serialize(),
        };

        if self.stream.write_all(&response_bytes).await.is_err() {
            eprintln!("error writing to stream");
        }
    }
}
