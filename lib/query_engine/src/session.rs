use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Result;
use tick_score::catalog::Catalog;
use tick_score::hot_buf::{SharedQuoteBuffers, SharedTradeBuffers};
use tokio::io::{AsyncReadExt, AsyncWriteExt, split};
use tokio::net::TcpStream;
use tokio::time::interval;

use crate::listen_config::ListenConfig;
use crate::query::run_query;

const READ_CHUNK_SIZE: usize = 4096;

pub struct QuerySession {
    stream: TcpStream,
    config: ListenConfig,
}

impl QuerySession {
    pub fn new(config: ListenConfig, stream: TcpStream) -> Self {
        Self { stream, config }
    }

    /// Serves one client connection: reads newline-delimited SQL queries,
    /// runs each against `catalog`+`hot_quotes`+`hot_trades`'s current data
    /// (hot ∪ cold) via `run_query`, and writes back CSV text terminated by
    /// a `--END--` line so a simple client (even `nc`) can tell where one
    /// response ends. Ends the session on a closed/errored connection, a
    /// failed write, or `config.timeout` seconds of inactivity.
    pub async fn run(
        self,
        catalog: Arc<RwLock<Catalog>>,
        hot_quotes: SharedQuoteBuffers,
        hot_trades: SharedTradeBuffers,
    ) -> Result<()> {
        let timeout_secs = self.config.timeout.unwrap_or(300).max(1) as u64;
        let mut idle_timeout = interval(Duration::from_secs(timeout_secs));
        idle_timeout.tick().await; // the first tick fires immediately; skip it

        let (mut reader, mut writer) = split(self.stream);
        let mut buf: Vec<u8> = Vec::new();
        let mut read_chunk = [0u8; READ_CHUNK_SIZE];

        loop {
            tokio::select! {
                read_result = reader.read(&mut read_chunk) => {
                    let n = match read_result {
                        Ok(0) | Err(_) => return Ok(()), // connection closed
                        Ok(n) => n,
                    };
                    idle_timeout.reset();
                    buf.extend_from_slice(&read_chunk[..n]);

                    while let Some(query) = extract_line(&mut buf) {
                        if query.is_empty() {
                            continue;
                        }
                        let response = match run_query(&catalog, &hot_quotes, &hot_trades, &query) {
                            Ok(csv) => csv,
                            Err(e) => format!("ERROR: {e}\n"),
                        };
                        if writer.write_all(response.as_bytes()).await.is_err() {
                            return Ok(());
                        }
                        if writer.write_all(b"--END--\n").await.is_err() {
                            return Ok(());
                        }
                    }
                }
                _ = idle_timeout.tick() => {
                    return Ok(()); // idle too long: close the connection
                }
            }
        }
    }
}

/// Extracts one newline-terminated line from `buf`, removing it (and the
/// newline) if a complete one has arrived, leaving a partial line for the
/// next read -- mirrors how `ingestion_engine`'s FIX framing pulls exactly
/// one complete message out of an accumulating buffer at a time.
fn extract_line(buf: &mut Vec<u8>) -> Option<String> {
    let pos = buf.iter().position(|&b| b == b'\n')?;
    let line: Vec<u8> = buf.drain(..=pos).collect();
    Some(String::from_utf8_lossy(&line[..line.len() - 1]).trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_line_returns_none_until_a_full_line_arrives() {
        let mut buf = b"SELECT * FROM".to_vec();
        assert_eq!(extract_line(&mut buf), None);
        assert_eq!(buf, b"SELECT * FROM");
    }

    #[test]
    fn extract_line_extracts_and_drains_exactly_one_line() {
        let mut buf = b"SELECT 1\nSELECT 2\npartial".to_vec();

        assert_eq!(extract_line(&mut buf).unwrap(), "SELECT 1");
        assert_eq!(extract_line(&mut buf).unwrap(), "SELECT 2");
        assert_eq!(extract_line(&mut buf), None);
        assert_eq!(buf, b"partial");
    }

    #[test]
    fn extract_line_trims_surrounding_whitespace() {
        let mut buf = b"  SELECT 1  \n".to_vec();
        assert_eq!(extract_line(&mut buf).unwrap(), "SELECT 1");
    }

    fn write_test_parquet(path: &std::path::Path, symbol: &str, price: f64) {
        use polars::prelude::*;
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut df = DataFrame::new(
            1,
            vec![
                Series::new("symbol".into(), &[symbol]).into(),
                Series::new("price".into(), &[price]).into(),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(path).unwrap();
        polars::io::parquet::write::ParquetWriter::new(file)
            .finish(&mut df)
            .unwrap();
    }

    /// End-to-end over a real socket: connects a client, sends one query,
    /// and checks the response is both correct and correctly framed
    /// (ends with the `--END--` marker the protocol promises).
    #[tokio::test(flavor = "multi_thread")]
    async fn serves_a_query_over_a_real_socket_and_frames_the_response() {
        let dir = tempfile::tempdir().unwrap();
        let mut inner = Catalog::new(dir.path().to_path_buf());
        let file = dir.path().join("t.parquet");
        write_test_parquet(&file, "BTC-USD", 100.0);
        inner.add_file(
            Arc::from("BTC-USD"),
            tick_score::catalog::Kind::Trade,
            chrono::Utc::now().date_naive(),
            file,
        );
        let catalog = Arc::new(RwLock::new(inner));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let config = ListenConfig {
                addr: None,
                port: None,
                timeout: Some(5),
            };
            let hot_quotes: SharedQuoteBuffers = Arc::new(RwLock::new(std::collections::HashMap::new()));
            let hot_trades: SharedTradeBuffers = Arc::new(RwLock::new(std::collections::HashMap::new()));
            let _ = QuerySession::new(config, stream).run(catalog, hot_quotes, hot_trades).await;
        });

        // A real client sends the tagged wire format (see `syntax`), not
        // raw SQL: TAG=VALUE pairs, each terminated by SOH (0x01).
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"1=BTC-USD\x012=SELECT * FROM trades WHERE symbol = 'BTC-USD'\x01\n")
            .await
            .unwrap();

        let mut response = Vec::new();
        let mut chunk = [0u8; 1024];
        loop {
            let n = client.read(&mut chunk).await.unwrap();
            assert!(n > 0, "server closed the connection before sending --END--");
            response.extend_from_slice(&chunk[..n]);
            if response.ends_with(b"--END--\n") {
                break;
            }
        }

        let response = String::from_utf8(response).unwrap();
        assert!(response.contains("BTC-USD,100.0"), "got: {response:?}");
        assert!(response.ends_with("--END--\n"));
    }
}
