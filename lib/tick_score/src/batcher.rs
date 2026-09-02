use std::collections::HashMap;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, NaiveDate, Utc};
use futures::{Stream, StreamExt};
use ingestion_engine::types::types::Tick;
use polars::error::PolarsResult;
use polars::io::parquet::write::ParquetWriteOptions;
use polars::lazy::dsl::{FileWriteFormat, SinkDestination, SinkTarget, UnifiedSinkArgs};
use polars::lazy::frame::{IntoLazy, LazyFrame};
use polars::prelude::PlRefPath;
use tokio::time::interval;

use crate::catalog::{Catalog, Kind};
use crate::compactor::Compactor;
use crate::config::batch_config::BatchConfig;
use crate::hot_buf::{HotBuf, HotBufQuote, HotBufTrade, SharedQuoteBuffers, SharedTradeBuffers};

pub async fn run(
    batch_config_file: &str,
    stream: Pin<Box<dyn Stream<Item = Result<Tick>> + Send + 'static>>,
    catalog: Arc<RwLock<Catalog>>,
    quotes: SharedQuoteBuffers,
    trades: SharedTradeBuffers,
) -> Result<()> {
    let batch_config = BatchConfig::new(batch_config_file)?;
    run_on_stream(batch_config, stream, catalog, quotes, trades).await
}

/// Consumes the decoded tick stream, grouping rows into one hot buffer per
/// symbol -- a separate map per kind (`quotes`, `trades`) rather than one
/// map keyed by `(Kind, Symbol)`, since a Quote's and a Trade's buffered
/// columns are different concrete types; which map a symbol's entry lives
/// in is what tells you its kind; no enum wrapper is needed to make one
/// `HashMap` hold both.
///
/// `catalog`, `quotes`, and `trades` are all supplied by the caller rather
/// than constructed here, so whatever else needs to see the same data (a
/// query engine, in particular) can share the exact same instances instead
/// of each side ending up with its own, disconnected view.
async fn run_on_stream(
    batch_config: BatchConfig,
    mut stream: Pin<Box<dyn Stream<Item = Result<Tick>> + Send + 'static>>,
    catalog: Arc<RwLock<Catalog>>,
    quotes: SharedQuoteBuffers,
    trades: SharedTradeBuffers,
) -> Result<()> {
    let max_rows = batch_config.max_rows;
    let max_age = Duration::from_secs((batch_config.max_age as u64).max(1));
    let compact_interval = Duration::from_secs((batch_config.compact_interval as u64).max(1));
    let compact_threshold = batch_config.compact_threshold;
    // Derived from the catalog itself, not batch_config.data_dir -- the
    // catalog is what actually decides where files live; keeping a second,
    // independently-configured copy of the same path is how the two end up
    // disagreeing.
    let data_dir = catalog.read().expect("catalog lock poisoned").data_dir().to_path_buf();

    let start_date = chrono::Utc::now();

    tokio::spawn(async move {
        let mut flush_check = interval(max_age);
        let compact_check = interval(compact_interval);
        let catalog_live = catalog;
        let compactor = Compactor::new(compact_check, catalog_live.clone(), compact_threshold);
        compactor.start();

        let _ = flush_check.tick().await;

        loop {
            tokio::select! {
                tick = stream.next() => {
                    let Some(tick) = tick else { break }; // stream closed
                    match tick {
                        Ok(Tick::Quote(quote)) => {
                            let symbol = quote.ticker_name.clone();
                            if !is_safe_symbol(&symbol) {
                                eprintln!("dropping quote for unsafe symbol {symbol:?}");
                                continue;
                            }
                            let mut guard = quotes.write().expect("quotes lock poisoned");
                            let buf = guard.entry(symbol.clone()).or_insert_with(|| (HotBufQuote::new(max_rows), 0));
                            buf.0.push(quote);
                            if buf.0.len() >= max_rows {
                                flush_quote(&symbol, buf, &catalog_live, &start_date, &data_dir);
                            }
                        }
                        Ok(Tick::Trade(trade)) => {
                            let symbol = trade.ticker_name.clone();
                            if !is_safe_symbol(&symbol) {
                                eprintln!("dropping trade for unsafe symbol {symbol:?}");
                                continue;
                            }
                            let mut guard = trades.write().expect("trades lock poisoned");
                            let buf = guard.entry(symbol.clone()).or_insert_with(|| (HotBufTrade::new(max_rows), 0));
                            buf.0.push(trade);
                            if buf.0.len() >= max_rows {
                                flush_trade(&symbol, buf, &catalog_live, &start_date, &data_dir);
                            }
                        }
                        // malformed/decoding error upstream: skip, keep the session alive
                        Err(_) => continue,
                    }
                }
                _ = flush_check.tick() => {
                    sweep_stale(&quotes, &catalog_live, &start_date, &data_dir, flush_quote);
                    sweep_stale(&trades, &catalog_live, &start_date, &data_dir, flush_trade);
                }
            }
        }
    });

    Ok(())
}

/// `symbol` originates from the exchange's raw FIX message body and later
/// becomes a filesystem path component when a partition is flushed to
/// parquet (see `flush_quote`). Reject anything that isn't plain
/// ticker-shaped text before it's ever buffered, so a malformed or
/// malicious SYMBOL value -- containing "../", a NUL byte, or looking like
/// an absolute path -- can never reach the write path (`Path::join` on an
/// absolute component silently discards everything joined before it).
fn is_safe_symbol(symbol: &str) -> bool {
    !symbol.is_empty()
        && symbol.len() <= 32
        && symbol
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// Flushes every key whose oldest row has been buffered for at least
/// `max_age`, regardless of row count -- the time backstop for a quiet
/// symbol. Written once against the shared `HotBuf` trait rather than
/// duplicated for `quotes` and `trades` separately.
fn sweep_stale<B: HotBuf>(
    buffers: &Arc<RwLock<HashMap<Arc<str>, (B, u32)>>>,
    catalog: &Arc<RwLock<Catalog>>,
    date: &DateTime<Utc>,
    data_dir: &Path,
    mut flush: impl FnMut(&Arc<str>, &mut (B, u32), &Arc<RwLock<Catalog>>, &DateTime<Utc>, &Path),
) {
    let mut guard = buffers.write().expect("hot buffer lock poisoned");
    for (symbol, buf) in guard.iter_mut() {
        if buf.0.is_empty() {
            continue;
        }

        flush(symbol, buf, catalog, date, data_dir);
    }
}

// TODO: replace with a real writer::flush() call (parquet file + FileCatalog
// update) once writer.rs exists -- for now this snapshots + assigns the next
// file number so the pipeline is wired end-to-end and compiles.
fn flush_quote(symbol: &Arc<str>, buf: &mut (HotBufQuote, u32), catalog: &Arc<RwLock<Catalog>>, date: &DateTime<Utc>, data_dir: &Path) {
    let (hot_buf, next_seq) = buf;
    let file_seq = *next_seq;
    *next_seq += 1;

    let tmp = hot_buf.take();
    let lf: LazyFrame = tmp.to_dataframe().lazy();
    let file_name = format!("part-{file_seq}.parquet");

    let out_path = get_output_path(data_dir, Kind::Quote, symbol.clone(), &file_name, &date.date_naive());

    // sink() only builds a LazyFrame whose plan ends in a sink step -- it
    // does no I/O itself. The write only happens once that plan is
    // executed, via collect() on the LazyFrame it returns.
    if let Err(e) = stream_saving(lf, &out_path) {
        eprintln!("failed to write parquet file {out_path:?}: {e}");
    } else {
        match catalog.write() {
            Ok(mut guard) => guard.add_file(symbol.clone(), Kind::Quote, date.date_naive(), out_path),
            Err(e) => eprintln!("catalog lock poisoned: {e}"),
        }
    }
}

fn flush_trade(symbol: &Arc<str>, buf: &mut (HotBufTrade, u32), catalog: &Arc<RwLock<Catalog>>, date: &DateTime<Utc>, data_dir: &Path) {
    let (hot_buf, next_seq) = buf;
    let file_seq = *next_seq;
    *next_seq += 1;

    let tmp = hot_buf.take();
    let lf: LazyFrame = tmp.to_dataframe().lazy();
    let file_name = format!("part-{file_seq}.parquet");

    let out_path = get_output_path(data_dir, Kind::Trade, symbol.clone(), &file_name, &date.date_naive());

    // sink() only builds a LazyFrame whose plan ends in a sink step -- it
    // does no I/O itself. The write only happens once that plan is
    // executed, via collect() on the LazyFrame it returns.
    if let Err(e) = stream_saving(lf, &out_path) {
        eprintln!("failed to write parquet file {out_path:?}: {e}");
    } else {
        match catalog.write() {
            Ok(mut guard) => guard.add_file(symbol.clone(), Kind::Trade, date.date_naive(), out_path),
            Err(e) => eprintln!("catalog lock poisoned: {e}"),
        }
    }
}

pub fn get_output_path(data_dir: &Path, kind: Kind, symbol: Arc<str>, file_name: &str, date: &NaiveDate) -> PathBuf {
    let base = data_dir.as_os_str();
    let label = kind.get_label();
    let date_str = date.to_string();

    let mut path = PathBuf::with_capacity(
        base.len() + label.len() + date_str.len() + symbol.len() + file_name.len() + 4,
    );
    path.push(data_dir);
    path.push(label);
    path.push(date_str);
    path.push(symbol.deref());
    path.push(file_name);
    path
}

pub fn stream_saving(lf: LazyFrame, save_path: &PathBuf) -> PolarsResult<()> {
    // sink() writes straight to `save_path` -- it does not create missing
    // parent directories itself, so the very first flush for a new
    // symbol/date partition would otherwise fail with a bare "No such file
    // or directory".
    if let Some(parent) = save_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let option = ParquetWriteOptions::default();
    let unified_args = UnifiedSinkArgs::default();

    // sink() only builds the plan; collect() is what actually executes the
    // write. Its result must be propagated, not discarded -- a caller that
    // deletes source files once this returns Ok(()) needs that to mean the
    // merged file is genuinely durable, not just that the plan was built.
    lf.sink(
        SinkDestination::File {
            target: SinkTarget::Path(PlRefPath::new(save_path.to_string_lossy())),
        },
        FileWriteFormat::Parquet(Arc::new(option)),
        unified_args,
    )?
    .collect()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ingestion_engine::types::types::{Quote, Trade};
    use tempfile::tempdir;

    fn sample_quote(receive_time: DateTime<Utc>, symbol: &str) -> Quote {
        Quote {
            receive_time,
            ticker_name: Arc::from(symbol),
            bid_price: 100.0,
            bid_vol: 10,
            ask_price: 100.5,
            ask_vol: 12,
        }
    }

    fn sample_trade(receive_time: DateTime<Utc>, symbol: &str) -> Trade {
        Trade {
            receive_time,
            ticker_name: Arc::from(symbol),
            trade_px: 100.0,
            trade_vol: 5,
            trd_type: Some(1),
        }
    }

    #[test]
    fn is_safe_symbol_accepts_plain_tickers() {
        assert!(is_safe_symbol("BTC-USD"));
        assert!(is_safe_symbol("AAPL"));
        assert!(is_safe_symbol("A.B_C-1"));
    }

    #[test]
    fn is_safe_symbol_rejects_traversal_and_malformed_input() {
        assert!(!is_safe_symbol(""));
        assert!(!is_safe_symbol("../../etc/passwd"));
        assert!(!is_safe_symbol("/etc/passwd"));
        assert!(!is_safe_symbol("a/b"));
        assert!(!is_safe_symbol(&"X".repeat(33)));
    }

    #[test]
    fn get_output_path_has_expected_shape() {
        let dir = tempdir().unwrap();
        let date = NaiveDate::from_ymd_opt(2026, 8, 31).unwrap();

        let path = get_output_path(dir.path(), Kind::Quote, Arc::from("BTC-USD"), "part-0.parquet", &date);

        assert_eq!(
            path,
            dir.path()
                .join("Quote")
                .join("2026-08-31")
                .join("BTC-USD")
                .join("part-0.parquet")
        );
    }

    #[test]
    fn flush_quote_writes_a_file_and_registers_it_in_the_catalog() {
        let dir = tempdir().unwrap();
        let catalog = Arc::new(RwLock::new(Catalog::new(dir.path().to_path_buf())));
        let symbol: Arc<str> = Arc::from("BTC-USD");
        let date = Utc::now();

        let mut hot_buf = HotBufQuote::new(4);
        hot_buf.push(sample_quote(date, &symbol));
        hot_buf.push(sample_quote(date, &symbol));
        let mut buf = (hot_buf, 0u32);

        flush_quote(&symbol, &mut buf, &catalog, &date, dir.path());

        // the sequence number advances and the buffer resets for the next flush
        assert_eq!(buf.1, 1);
        assert_eq!(buf.0.len(), 0);

        let guard = catalog.read().unwrap();
        let files = guard.files_for(symbol.clone(), Kind::Quote, date.date_naive());
        assert_eq!(files.len(), 1);
        assert!(files[0].exists());

        let written = LazyFrame::scan_parquet(
            PlRefPath::try_from_path(&files[0]).unwrap(),
            polars::lazy::frame::ScanArgsParquet::default(),
        )
        .unwrap()
        .collect()
        .unwrap();
        assert_eq!(written.height(), 2);
    }

    #[test]
    fn flush_trade_registers_under_trade_kind_not_quote() {
        let dir = tempdir().unwrap();
        let catalog = Arc::new(RwLock::new(Catalog::new(dir.path().to_path_buf())));
        let symbol: Arc<str> = Arc::from("BTC-USD");
        let date = Utc::now();

        let mut hot_buf = HotBufTrade::new(4);
        hot_buf.push(sample_trade(date, &symbol));
        let mut buf = (hot_buf, 0u32);

        flush_trade(&symbol, &mut buf, &catalog, &date, dir.path());

        let guard = catalog.read().unwrap();
        assert_eq!(guard.files_for(symbol.clone(), Kind::Trade, date.date_naive()).len(), 1);
        assert!(guard.files_for(symbol, Kind::Quote, date.date_naive()).is_empty());
    }
}
