use anyhow::Result;
use clap::Parser;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[derive(Parser)]
#[command(about)]
struct Cli {
    /// Port to listen on
    #[arg(short, long)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let listener = TcpListener::bind(("localhost", cli.port)).await?;
    println!("Listening on port {}", cli.port);

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            process_client_login(socket).await;
        });
    }
}

async fn process_client_login(mut socket: TcpStream) {
    loop {
        let mut buffer = [0; 1024];
        let read_bytes = match socket.read(&mut buffer).await {
            Ok(read_bytes) if read_bytes != 0 => read_bytes,
            _ => {
                println!("Connection closed by client");
                break;
            }
        };
        if socket.write(&buffer[..read_bytes]).await.is_err() {
            println!("Connection closed by client");
            break;
        }
    }
}
