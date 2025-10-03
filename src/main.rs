#![allow(unused_imports)]
mod client;
mod commands;
mod macros;
mod server;
mod store;
mod types;
mod utils;

use server::RedisServer;
use tokio::io::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let server = RedisServer::new(String::from("127.0.0.1:6379"));
    server.run().await;
    Ok(())
}
