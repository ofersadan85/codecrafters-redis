use std::net::Ipv4Addr;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[arg(long, default_value = "0.0.0.0")]
    pub host: Ipv4Addr,

    #[arg(short, long, default_value = "6379")]
    pub port: u16,
}
