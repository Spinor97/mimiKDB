pub mod session;
pub mod listen_config;
pub mod query;
pub mod syntax;

use std::sync::{Arc, RwLock};

use anyhow::Result;
use tick_score::catalog::Catalog;
use tick_score::hot_buf::{SharedQuoteBuffers, SharedTradeBuffers};
use tokio::net::TcpListener;

use crate::listen_config::ListenConfig;
use crate::session::QuerySession;

/// Binds a TCP listener per `listen_config_file` and, for as long as the
/// process runs, accepts client connections and spawns one `QuerySession`
/// per client -- each session can issue any number of newline-delimited
/// SQL queries against `catalog`+`hot_quotes`+`hot_trades`'s current data.
/// Share the exact same handles passed to `tick_score::batcher::run` so
/// queries here see the same hot-and-cold data the batcher is writing.
pub async fn run(
    listen_config_file: &str,
    catalog: Arc<RwLock<Catalog>>,
    hot_quotes: SharedQuoteBuffers,
    hot_trades: SharedTradeBuffers,
) -> Result<()> {
    let config = ListenConfig::new(listen_config_file)?;
    let addr = format!(
        "{}:{}",
        config.addr.clone().unwrap_or_else(|| "0.0.0.0".to_string()),
        config.port.clone().unwrap_or_else(|| "5432".to_string()),
    );
    let listener = TcpListener::bind(&addr).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        let session = QuerySession::new(config.clone(), stream);
        let catalog = catalog.clone();
        let hot_quotes = hot_quotes.clone();
        let hot_trades = hot_trades.clone();
        tokio::spawn(async move {
            if let Err(e) = session.run(catalog, hot_quotes, hot_trades).await {
                eprintln!("query session ended with error: {e}");
            }
        });
    }
}
