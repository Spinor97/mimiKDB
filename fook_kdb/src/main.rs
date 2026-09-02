use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result, bail};
use ingestion_engine::network::session;
use tick_score::batcher;
use tick_score::catalog::Catalog;
use tick_score::config::batch_config::BatchConfig;

// Must stay multi-threaded: Polars' sink()/collect() (used by every parquet
// write, in the batcher and the compactor) panics ("can call blocking only
// when running on the multi-threaded runtime") on tokio's single-threaded
// flavor. Discovered the hard way in tick_score's own tests -- do not
// change this to "current_thread" without re-verifying that constraint.
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);

    let logon_config_file = args.next().unwrap_or_else(|| "logon.yaml".to_string());
    let batch_config_file = args.next().unwrap_or_else(|| "batch_config.yaml".to_string());
    let listen_config_file = args.next().unwrap_or_else(|| "listen_config.yaml".to_string());
    let symbols: Vec<String> = match args.next() {
        Some(s) => s.split(',').map(|s| s.trim().to_string()).collect(),
        None => {
            eprintln!(
                "usage: fook_kdb [logon_config] [batch_config] [listen_config] <symbol1,symbol2,...>\n\
                 (defaults: logon.yaml, batch_config.yaml, listen_config.yaml -- see the .example.yaml files)"
            );
            bail!("at least one symbol is required");
        }
    };

    // BatchConfig is read here (not just inside tick_score::batcher::run,
    // which re-reads it for its own fields) so data_dir can seed the
    // Catalog before anything runs -- the batcher derives its own working
    // data_dir back out of the Catalog it's given, rather than trusting a
    // second, independently-configured copy of the same path.
    let batch_config = BatchConfig::new(&batch_config_file)
        .with_context(|| format!("failed to load batch config from {batch_config_file:?}"))?;
    let data_dir = PathBuf::from(&batch_config.data_dir);

    let catalog = Arc::new(RwLock::new(Catalog::new(data_dir)));
    let hot_quotes: tick_score::hot_buf::SharedQuoteBuffers = Arc::new(RwLock::new(HashMap::new()));
    let hot_trades: tick_score::hot_buf::SharedTradeBuffers = Arc::new(RwLock::new(HashMap::new()));

    println!("connecting using {logon_config_file:?} for symbols: {symbols:?}");
    let stream = session::run(&logon_config_file, symbols)
        .await
        .with_context(|| format!("failed to connect using logon config {logon_config_file:?}"))?;

    batcher::run(&batch_config_file, stream, catalog.clone(), hot_quotes.clone(), hot_trades.clone())
        .await
        .context("failed to start the batcher")?;

    println!("batcher running -- starting the query server using {listen_config_file:?}");

    // Loops forever accepting connections; this is what keeps the process
    // alive once ingestion and batching are both running in the background.
    query_engine::run(&listen_config_file, catalog, hot_quotes, hot_trades)
        .await
        .context("query server exited with an error")
}
