#![allow(unused_imports)]
mod commands;
mod types;
mod server;

use server::RedisServer;
use tokio::io::Result;


#[tokio::main]
async fn main() -> Result<()> {
    let server = RedisServer::new("127.0.0.1:6379").await?;
    server.run().await;
    Ok(())
}