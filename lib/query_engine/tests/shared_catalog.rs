//! Proves the actual point of sharing one `Arc<RwLock<Catalog>>` (plus the
//! hot buffer handles) between `tick_score`'s batcher and `query_engine`'s
//! query path: data written by the batcher -- flushed *or* still hot --
//! becomes visible to a query, with no socket or real exchange connection
//! involved, just a fake tick stream feeding the batcher and a direct SQL
//! query against the same shared handles.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use futures::stream;
use ingestion_engine::types::types::{Quote, Tick};
use query_engine::query::run_query;
use tick_score::catalog::Catalog;
use tokio::time::{Duration, sleep};

fn sample_quote(symbol: &str, bid_price: f32, ask_price: f32) -> Quote {
    Quote {
        receive_time: chrono::Utc::now(),
        ticker_name: Arc::from(symbol),
        bid_price,
        bid_vol: 10,
        ask_price,
        ask_vol: 12,
    }
}

/// Builds a wire message the way a real client would: `TAG=VALUE` pairs,
/// each terminated by SOH (0x01) -- see `query_engine::syntax`.
fn sql_message(symbol: &str, sentence: &str) -> String {
    format!("1={symbol}\u{1}2={sentence}\u{1}")
}

fn write_batch_config(dir: &std::path::Path, max_rows: usize) -> String {
    let path = dir.join("batch_config.yaml");
    let data_dir = dir.to_str().unwrap();
    std::fs::write(
        &path,
        format!(
            "max_rows: {max_rows}\nmax_age: 3600\ndata_dir: \"{data_dir}\"\ncompact_interval: 3600\ncompact_threshold: 1000\n"
        ),
    )
    .unwrap();
    path.to_str().unwrap().to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_flushed_tick_becomes_queryable_through_the_shared_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(RwLock::new(Catalog::new(dir.path().to_path_buf())));
    let hot_quotes = Arc::new(RwLock::new(HashMap::new()));
    let hot_trades = Arc::new(RwLock::new(HashMap::new()));

    // max_rows = 2, and exactly 2 quotes pushed -- triggers exactly one flush.
    let ticks: Vec<anyhow::Result<Tick>> = vec![
        Ok(Tick::Quote(sample_quote("BTC-USD", 100.0, 100.5))),
        Ok(Tick::Quote(sample_quote("BTC-USD", 101.0, 101.5))),
    ];
    let tick_stream = Box::pin(stream::iter(ticks));

    let config_path = write_batch_config(dir.path(), 2);
    tick_score::batcher::run(&config_path, tick_stream, catalog.clone(), hot_quotes.clone(), hot_trades.clone())
        .await
        .unwrap();

    // batcher::run spawns its work and returns immediately -- give the
    // spawned task a moment to actually drain the stream and flush.
    sleep(Duration::from_millis(300)).await;

    let csv = run_query(
        &catalog,
        &hot_quotes,
        &hot_trades,
        &sql_message("BTC-USD", "SELECT * FROM quotes WHERE symbol = 'BTC-USD' ORDER BY bid_price"),
    )
    .unwrap();
    let mut lines = csv.lines();

    // `symbol` is stamped on by scan_kind (from the catalog's partition
    // key), not a column the writer stores per row -- it lands appended
    // after the written columns.
    assert_eq!(
        lines.next().unwrap(),
        "receive_time,bid_price,bid_vol,ask_price,ask_vol,symbol"
    );
    let row1 = lines.next().expect("first quote row");
    let row2 = lines.next().expect("second quote row");
    assert!(row1.ends_with(",100.0,10,100.5,12,BTC-USD"), "got: {row1:?}");
    assert!(row2.ends_with(",101.0,10,101.5,12,BTC-USD"), "got: {row2:?}");
    assert_eq!(lines.next(), None, "expected exactly 2 rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unflushed_tick_is_already_queryable_through_the_shared_hot_buffer() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(RwLock::new(Catalog::new(dir.path().to_path_buf())));
    let hot_quotes = Arc::new(RwLock::new(HashMap::new()));
    let hot_trades = Arc::new(RwLock::new(HashMap::new()));

    // max_rows = 10, only 1 quote pushed -- never reaches the flush
    // threshold, so this row never becomes a parquet file. It's still
    // queryable, though, because the batcher and the query engine share
    // the same hot_quotes handle -- this is the whole point of wiring it
    // through instead of leaving it batcher-local.
    let ticks: Vec<anyhow::Result<Tick>> = vec![Ok(Tick::Quote(sample_quote("BTC-USD", 100.0, 100.5)))];
    let tick_stream = Box::pin(stream::iter(ticks));

    let config_path = write_batch_config(dir.path(), 10);
    tick_score::batcher::run(&config_path, tick_stream, catalog.clone(), hot_quotes.clone(), hot_trades.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(300)).await;

    // Confirm it really hasn't flushed: nothing tracked in the catalog yet.
    assert!(catalog.read().unwrap().all_files(tick_score::catalog::Kind::Quote).is_empty());

    let csv = run_query(
        &catalog,
        &hot_quotes,
        &hot_trades,
        &sql_message("BTC-USD", "SELECT bid_price FROM quotes WHERE symbol = 'BTC-USD'"),
    )
    .unwrap();
    assert_eq!(csv.trim(), "bid_price\n100.0".trim());
}
