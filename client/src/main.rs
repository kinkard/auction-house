use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let addr = std::env::args()
        .nth(1)
        .context("Usage: client <addr:port>\nExample: cargo run localhost:3000")?;

    let stream = tokio::net::TcpStream::connect(&addr).await?;
    let (mut tcp_reader, mut tcp_writer) = tokio::io::split(stream);

    let tcp_reader_task = tokio::spawn(async move {
        let mut buffer = [0; 1024];
        loop {
            let read_bytes = match tcp_reader.read(&mut buffer).await {
                Ok(read_bytes) if read_bytes != 0 => read_bytes,
                _ => {
                    println!("Connection closed by server");
                    break;
                }
            };
            match std::str::from_utf8(&buffer[..read_bytes]) {
                Ok(response) => println!("> {response}"),
                Err(err) => println!("Non-utf8: {err}> {:?}", &buffer[..read_bytes]),
            }
        }
    });

    let (cli_tx, cli_rx) = flume::bounded(1);

    let tcp_writer_task = tokio::spawn(async move {
        loop {
            let input: String = cli_rx
                .recv_async()
                .await
                .expect("tx channel should always outlive rx");
            if tcp_writer.write(input.as_bytes()).await.is_err() {
                println!("Connection closed by server");
                break;
            }
        }
    });

    // We use a separate thread to read user input as a workaround for a know issue with blocking stdin.
    // Once one of the tasks completes (i.e. the server closes the connection), everything will be
    // canceled when the main task completes
    std::thread::spawn(move || loop {
        let mut buffer = String::new();
        std::io::stdin().read_line(&mut buffer).unwrap();
        if buffer.ends_with('\n') {
            buffer.pop(); // `read_line` preserves the newline symbol that we don't need
        }
        if cli_tx.send(buffer).is_err() {
            break; // channel was closed, time to exit
        }
    });

    tokio::select! {
        _ = tcp_reader_task => {},
        _ = tcp_writer_task => {},
    };
    Ok(())
}
