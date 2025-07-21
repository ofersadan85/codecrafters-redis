use anyhow::Context;
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    select,
};
use tracing::{debug, error, info, instrument, warn};

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
        let s = String::from_utf8_lossy(&buf);
        for line in s.lines() {
            if line.starts_with("PING") {
                let mut message = line.trim_start_matches("PING").trim();
                if message.is_empty() {
                    message = "PONG";
                }
                let message = format!("+{}\r\n", message);
                debug!("Responding with {message}");
                stream
                    .write_all(message.as_bytes())
                    .await
                    .context("Failed to write response")?;
            } else {
                warn!("Unsupported command ({n}): {line}");
                // bail!("Unsupported command ({n}): {line}");
            }
        }
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
