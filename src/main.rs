use anyhow::Context;
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    select,
    sync::Mutex,
    time::sleep,
};
use tracing::{debug, error, info, instrument, warn};

use crate::{cmd::Command, resp::RespData};

mod cmd;
mod resp;

type KeyValueStore = Arc<Mutex<HashMap<String, String>>>;

async fn expire_key(kv: KeyValueStore, key: String, duration: Duration) {
    sleep(duration).await;
    kv.lock().await.remove(&key);
}

#[instrument(skip(stream))]
async fn handle_client(
    mut stream: TcpStream,
    client: SocketAddr,
    kv: KeyValueStore,
) -> anyhow::Result<()> {
    // let mut stream = BufStream::new(stream);
    let mut buf = [0; 1024];
    loop {
        let n = stream.read(&mut buf[..]).await?;
        if n == 0 {
            info!("Disconnected");
            return Ok(());
        }
        debug!("{}", String::from_utf8_lossy(&buf[..n]));
        let command =
            Command::try_from(&buf[..n]).context("Failed to parse command from buffer")?;
        debug!("Parsed command: {:?}", command);
        let response = match command {
            Command::Ping => RespData::simple_string("PONG"),
            Command::Echo(arg) => RespData::bulk_string(&arg),
            Command::Set {
                key,
                value,
                expires,
                args: _args
            } => {
                // Here you would typically set the key-value pair in a store
                debug!("Setting `{key}` to `{value}`");
                kv.lock().await.insert(key.clone(), value);
                if let Some(expires) = expires {
                    tokio::spawn(expire_key(kv.clone(), key, expires));
                }
                RespData::simple_string("OK")
            }
            Command::Get(key) => {
                // Here you would typically get the value for the key from a store
                debug!("Getting value for key: {}", key);
                let store = kv.lock().await;
                if let Some(value) = store.get(&key) {
                    RespData::bulk_string(value)
                } else {
                    RespData::null_bulk_string()
                }
            }
        };
        debug!("Responding with {response}");
        stream
            .write_all(response.as_bytes().as_slice())
            .await
            .context("Failed to write response")?;
    }
}

async fn handle_ctrl_c() -> anyhow::Result<()> {
    tokio::signal::ctrl_c()
        .await
        .context("Failed to listen for Ctrl+C")?;
    info!("Received Ctrl+C, shutting down...");
    std::process::exit(0);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let kv = Arc::new(Mutex::new(HashMap::new()));
    tracing_subscriber::fmt()
        .with_env_filter("debug")
        // .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .init();
    let listener = TcpListener::bind("127.0.0.1:6379")
        .await
        .context("Failed to bind to address")?;
    info!("Server listening on {}", listener.local_addr()?);
    loop {
        select! {
            _ = handle_ctrl_c() => {}
            connection = listener.accept() => {
                match connection {
                    Ok((stream, client)) => {
                        info!("Accepted connection from {client}");
                        let kv = kv.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_client(stream, client, kv).await {
                                error!("Error handling client: {e}");
                            }
                        });
                    }
                    Err(e) => {
                        warn!("Failed to accept connection: {e}");
                    }
                }
            }
        }
    }
}
