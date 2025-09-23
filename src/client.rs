use std::{hash::Hash, sync::Arc};

use nanoid::nanoid;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Notify,
    time::Instant,
};

use crate::types::RedisType;

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

    pub async fn write_response(&mut self, value: &Option<RedisType>)
    where
        T: AsyncReadExt + AsyncWriteExt + Unpin + Send,
    {
        if let Some(val) = value {
            if self.tcp_stream.write_all(&val.serialize()).await.is_err() {
                eprintln!("error writing to stream");
            }
        } else {
            if self
                .tcp_stream
                .write_all(&RedisType::Null.serialize())
                .await
                .is_err()
            {
                eprintln!("error writing to stream");
            }
        }
    }
}
