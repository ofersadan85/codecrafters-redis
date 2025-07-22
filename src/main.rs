use anyhow::{bail, Context};
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    select,
};
use tracing::{debug, error, info, instrument, warn};

use crate::resp::RespData;

mod resp;

#[instrument(skip(stream))]
async fn handle_client(mut stream: TcpStream, client: SocketAddr) -> anyhow::Result<()> {
    // let mut stream = BufStream::new(stream);
    let mut buf = [0; 1024];
    loop {
        let n = stream.read(&mut buf[..]).await?;
        if n == 0 {
            info!("Disconnected");
            return Ok(());
        }
        debug!("{}", String::from_utf8_lossy(&buf[..n]));
        let msg = RespData::try_from(&mut &buf[..n]).context("Failed to parse RESP data")?;
        let response = match msg {
            RespData::Array(Some(elements)) => match elements.get(0).unwrap() {
                RespData::BulkString(Some(cmd)) if cmd == b"PING" => "+PONG\r\n".to_string(),
                RespData::BulkString(Some(cmd)) if cmd == b"ECHO" => {
                    let Some(RespData::BulkString(Some(inner))) = elements.get(1) else {
                        bail!("ECHO command requires a bulk string argument");
                    };
                    format!(
                        "${}\r\n{}\r\n",
                        inner.len(),
                        String::from_utf8_lossy(&inner)
                    )
                }
                _ => bail!("Unsupported command"),
            },
            _ => bail!("Unsupported command"),
        };
        debug!("Responding with {response}");
        stream
            .write_all(response.as_bytes())
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
                        tokio::spawn(async move {
                            if let Err(e) = handle_client(stream, client).await {
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
