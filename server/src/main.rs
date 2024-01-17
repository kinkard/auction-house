use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

use storage::Storage;

mod commands;
mod storage;

// TcpStream reader half wrapper with buffer
struct TcpReader {
    stream: tokio::io::ReadHalf<TcpStream>,
    buf: [u8; 256],
}

impl TcpReader {
    fn new(stream_reader: tokio::io::ReadHalf<TcpStream>) -> Self {
        Self {
            stream: stream_reader,
            buf: [0; 256],
        }
    }

    async fn read<'a>(&'a mut self) -> Result<&'a [u8]> {
        let read_bytes = self.stream.read(&mut self.buf).await?;
        if read_bytes == 0 {
            return Err(anyhow!("Client disconnected"));
        }
        Ok(&self.buf[..read_bytes])
    }
}

#[derive(Parser)]
#[command(about)]
struct Cli {
    /// Port to listen on
    #[arg(short, long)]
    port: u16,

    /// Path to the database file. Example: db.sqlite
    #[arg(short, long)]
    db: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let storage = Arc::new(Mutex::new(Storage::open(&cli.db)?));

    let listener = TcpListener::bind(("localhost", cli.port)).await?;
    println!("Listening on port {}", cli.port);

    loop {
        let (socket, _) = listener.accept().await?;
        let storage = storage.clone();

        tokio::spawn(async move {
            let (tcp_reader, mut tcp_writer) = tokio::io::split(socket);
            let mut tcp_reader = TcpReader::new(tcp_reader);

            let _user = match process_client_login(&mut tcp_reader, &mut tcp_writer, storage).await
            {
                Ok(user) => {
                    println!("{user:?} successfully logged in",);
                    user
                }
                Err(err) => {
                    println!("Failed to process client login: {err}");
                    return;
                }
            };

            loop {
                let request = match tcp_reader.read().await {
                    Ok(request) => request,
                    _ => {
                        println!("Connection closed by client");
                        break;
                    }
                };

                let response = std::str::from_utf8(request)
                    .map_err(|err| anyhow!("{request:?} is not a valid utf8 string: {err}"))
                    .and_then(|request| commands::process_request(request))
                    .unwrap_or_else(|err| format!("Failed to process request: {err}"));

                if tcp_writer.write(response.as_bytes()).await.is_err() {
                    println!("Connection closed by client");
                    break;
                }
            }
        });
    }
}

async fn try_login(storage: Arc<Mutex<Storage>>, username: &[u8]) -> Result<storage::User> {
    let username = std::str::from_utf8(username)
        .context(format!("Invalid utf8 string: {:?}", username))?
        .trim();

    let storage = storage.lock().await;
    let user = storage.login(username)?;

    Ok(user)
}

async fn process_client_login(
    tcp_reader: &mut TcpReader,
    tcp_writer: &mut tokio::io::WriteHalf<TcpStream>,
    storage: Arc<Mutex<Storage>>,
) -> Result<storage::User> {
    tcp_writer
        .write(b"Welcome to Sundris Auction House, stranger! How can I call you?")
        .await?;

    let response = tcp_reader.read().await?;
    match try_login(storage, response).await {
        Ok(user) => {
            tcp_writer
                .write(format!("Successfully logged in as {}", user.username).as_bytes())
                .await?;
            Ok(user)
        }
        Err(err) => {
            // ignore write errors, we're already in a bad state
            let _ = tcp_writer
                .write(format!("Failed to login: {err}").as_bytes())
                .await;
            Err(err)
        }
    }
}
