use std::{hash::Hash, sync::Arc};

use nanoid::nanoid;

use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream},
    net::TcpStream,
    sync::Notify,
    time::Instant,
};

use crate::connection::Connection;

#[derive(Debug)]
pub struct RedisClient<T: AsyncRead + AsyncWrite + Unpin + Send> {
    pub id: String,
    pub created_at: Instant,
    // TODO - transformar num channel
    // https://tokio.rs/tokio/tutorial/channels
    pub notifier: Arc<Notify>,
    pub connection: Connection<T>,
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> PartialEq for RedisClient<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.created_at == other.created_at
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Eq for RedisClient<T> {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Hash for RedisClient<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.created_at.hash(state);
    }
}

impl RedisClient<TcpStream> {
    pub fn new(stream: TcpStream) -> Self {
        return Self {
            id: nanoid!(),
            created_at: Instant::now(),
            notifier: Arc::new(Notify::new()),
            connection: Connection::new(stream),
        };
    }
}

#[cfg(test)]
impl RedisClient<DuplexStream> {
    pub fn mock_new(stream: DuplexStream) -> Self {
        Self {
            id: nanoid!(),
            created_at: Instant::now(),
            notifier: Arc::new(Notify::new()),
            connection: Connection::new(stream),
        }
    }
}
