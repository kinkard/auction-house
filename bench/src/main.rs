use anyhow::Result;
use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Parser)]
#[command(about)]
struct Cli {
    /// Address to connect to. Example: localhost:3000
    #[arg(value_name = "addr:port")]
    addr: String,
}

struct Client {
    tcp_stream: tokio::net::TcpStream,
    read_buffer: [u8; 1024],
}

impl Client {
    async fn connect(addr: &str) -> Result<Self> {
        let tcp_stream = tokio::net::TcpStream::connect(addr).await?;

        Ok(Self {
            tcp_stream,
            read_buffer: [0; 1024],
        })
    }

    async fn login(&mut self, name: &str) -> Result<()> {
        let greeting = self.read().await?;
        if !greeting.starts_with("Welcome to Sundris Auction House, stranger!") {
            return Err(anyhow::anyhow!("Unexpected greeting: {greeting}"));
        }
        self.execute(name).await
    }

    async fn execute(&mut self, command: &str) -> Result<()> {
        self.tcp_stream.write(command.as_bytes()).await?;
        let response = self.read().await?;
        if response.starts_with("Successfully") {
            Ok(())
        } else {
            Err(anyhow::anyhow!("Command failed with error: {response}"))
        }
    }

    async fn read(&mut self) -> Result<&str> {
        let read_bytes = self.tcp_stream.read(&mut self.read_buffer).await?;
        match std::str::from_utf8(&self.read_buffer[..read_bytes]) {
            Ok(response) => Ok(response),
            Err(err) => Err(anyhow::anyhow!(
                "Non-utf8: {err}> {:?}",
                &self.read_buffer[..read_bytes]
            )),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    {
        println!("Executing 100k single client deposit/withdraw benchmark...");
        let mut miner = Client::connect(&cli.addr).await?;
        miner.login("Miner").await?;

        let begin = std::time::Instant::now();
        for _ in 0..100000 {
            miner.execute("deposit ore 10").await?;
            miner.execute("withdraw ore 10").await?;
        }
        let elapsed_sec = begin.elapsed().as_secs_f64();
        println!(
            "deposit/withdraw 100_000 times took: {:.3} seconds with {:.0} rps",
            elapsed_sec,
            200_000.0 / elapsed_sec
        );
    }

    {
        println!("Executing 100k single client deposit/withdraw (additinal) benchmark...");
        let mut miner = Client::connect(&cli.addr).await?;
        miner.login("Miner").await?;
        miner.execute("deposit ore 1").await?;

        let begin = std::time::Instant::now();
        for _ in 0..100000 {
            miner.execute("deposit ore 10").await?;
            miner.execute("withdraw ore 10").await?;
        }
        let elapsed_sec = begin.elapsed().as_secs_f64();
        println!(
            "deposit/withdraw additional 100_000 times took: {:.3} seconds with {:.0} rps",
            elapsed_sec,
            200_000.0 / elapsed_sec
        );
    }

    {
        println!("Executing 100k 20 client deposit/withdraw benchmark...");
        let begin = std::time::Instant::now();

        let mut clients = Vec::new();
        for i in 0..10 {
            let mut client = Client::connect(&cli.addr).await?;
            client.login(&format!("Client{}", i)).await?;
            clients.push(tokio::spawn(async move {
                for _ in 0..10_000 {
                    client.execute("deposit ore 10").await?;
                    client.execute("withdraw ore 10").await?;
                }
                Ok::<_, anyhow::Error>(())
            }));
        }

        for client in clients {
            client.await??;
        }
        let elapsed_sec = begin.elapsed().as_secs_f64();
        println!(
            "deposit/withdraw 100_000 times with 10 clients took: {:.3} seconds with {:.0} rps",
            elapsed_sec,
            200_000.0 / elapsed_sec
        );
    }

    Ok(())
}
