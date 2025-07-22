use anyhow::Context;
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    select,
    sync::Mutex,
};
use tracing::{debug, error, info, instrument, warn};

mod cmd;
mod resp;
mod state;

use crate::{cmd::Command, state::{AppState, State}};

#[instrument(skip(stream, state))]
async fn handle_client(
    mut stream: TcpStream,
    client: SocketAddr,
    state: State,
) -> anyhow::Result<()> {
    // let mut stream = BufStream::new(stream);
    let mut buf = [0; 1024];
    loop {
        let n = stream.read(&mut buf[..]).await?;
        if n == 0 {
            info!("Disconnected");
            return Ok(());
        }
        let command =
            Command::try_from(&buf[..n]).context("Failed to parse command from buffer")?;
        debug!("Parsed command: {command:?}");
        let response = command.handle(state.clone()).await?;
        debug!("Response: {response:?}");
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
    let state = Arc::new(Mutex::new(AppState::default()));
    tracing_subscriber::fmt()
        .with_env_filter("debug")
        .without_time()
        // .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .init();
    let listener = TcpListener::bind("0.0.0.0:6379")
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
                        let state = state.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_client(stream, client, state).await {
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
