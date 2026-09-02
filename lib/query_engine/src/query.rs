use std::sync::{Arc, RwLock};

use anyhow::{Result, anyhow};
use chrono::{Duration, NaiveDate};
use ingestion_engine::fix::parser::FixParser;
use ingestion_engine::fix::raw_msg::{RawMessage, ValType};
use polars::prelude::*;
use polars::sql::SQLContext;
use tick_score::catalog::{Catalog, Kind};
use tick_score::hot_buf::{SharedQuoteBuffers, SharedTradeBuffers, empty_quote_frame, empty_trade_frame, hot_lazyframe_for_symbol};

use crate::syntax;

/// Parses one query message off the wire into its tagged fields (see
/// `syntax`) -- the same `TAG=VALUE<SOH>...` shape `ingestion_engine`
/// already uses for real FIX messages, reused here rather than inventing a
/// second wire format. A message that isn't well-formed this way (or isn't
/// this format at all) simply yields fewer fields than expected; `FixParser`
/// itself never panics on it (see `ingestion_engine::fix::parser`), and the
/// specific `extract_*` calls below turn "field missing" into a clear error.
fn extract_info(query: &str) -> RawMessage {
    let mut parser = FixParser::new(query.as_bytes());
    let mut rtn = RawMessage::new(5);

    while let Some(field) = parser.next_field() {
        let _ = rtn.append_single(&field);
    }

    rtn
}

fn extract_query_type(query_info: &RawMessage) -> &str {
    match query_info.get_val(syntax::QUERY_TYPE) {
        Some(ValType::Single(s)) => s.as_str(),
        // Omitted: default to a plain SQL command rather than reject the
        // message outright -- QUERY_TYPE only matters once there's more
        // than one kind of command to choose between.
        _ => syntax::query_type::SQL_CMD,
    }
}

fn extract_symbol(query_info: &RawMessage) -> Result<Arc<str>> {
    match query_info.get_val(syntax::SYMBOL) {
        Some(ValType::Single(s)) => Ok(Arc::from(s.as_str())),
        _ => Err(anyhow!("query must specify a symbol (tag {})", syntax::SYMBOL)),
    }
}

fn extract_sentence(query_info: &RawMessage) -> Result<&str> {
    match query_info.get_val(syntax::SENTENCE) {
        Some(ValType::Single(s)) => Ok(s.as_str()),
        _ => Err(anyhow!("query must include a SQL sentence (tag {})", syntax::SENTENCE)),
    }
}

fn extract_date(query_info: &RawMessage, tag: u32) -> Result<Option<NaiveDate>> {
    match query_info.get_val(tag) {
        Some(ValType::Single(s)) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map(Some)
            .map_err(|e| anyhow!("invalid date '{s}' (tag {tag}), expected YYYY-MM-DD: {e}")),
        _ => Ok(None),
    }
}

/// Resolves the (start, end) date window a query scans, both bounds
/// inclusive, from whatever start_date/end_date were supplied (both, one,
/// or neither) plus the current date:
///
/// - Neither given: just `today`.
/// - Only `end` given: the 5 days up to and including `end`.
/// - Only `start` given: the 5 days from `start` forward.
/// - Both given: exactly as given (no 5-day cap -- that only exists to
///   fill in the missing side of a one-sided window), tolerant of the two
///   being supplied in either order.
fn resolve_date_range(start: Option<NaiveDate>, end: Option<NaiveDate>, today: NaiveDate) -> (NaiveDate, NaiveDate) {
    match (start, end) {
        (None, None) => (today, today),
        (None, Some(end)) => (end - Duration::days(4), end),
        (Some(start), None) => (start, start + Duration::days(4)),
        (Some(a), Some(b)) => (a.min(b), a.max(b)),
    }
}

/// Builds a fresh SQLContext with `trades`/`quotes` registered as exactly
/// one symbol's data in `[start, end]` -- only the files and hot buffer
/// entry for `symbol`, never any other symbol's. The hot buffer is always
/// included regardless of the requested date window (it only ever holds
/// today's not-yet-flushed rows, and a query should never lose sight of
/// what's currently live just because it also asked for older history).
/// This is what makes the query both "live" (a tick that arrived a moment
/// ago and hasn't flushed yet is still visible) and cheap (nothing outside
/// the requested symbol, and outside its cold files' date window, is ever
/// opened or scanned).
fn build_context(
    catalog: &Catalog,
    hot_quotes: &SharedQuoteBuffers,
    hot_trades: &SharedTradeBuffers,
    symbol: &Arc<str>,
    start: NaiveDate,
    end: NaiveDate,
) -> PolarsResult<SQLContext> {
    let ctx = SQLContext::new();
    ctx.register(
        "trades",
        scan_kind(catalog, Kind::Trade, symbol, start, end, hot_lazyframe_for_symbol(hot_trades, symbol)?)?,
    );
    ctx.register(
        "quotes",
        scan_kind(catalog, Kind::Quote, symbol, start, end, hot_lazyframe_for_symbol(hot_quotes, symbol)?)?,
    );
    Ok(ctx)
}

fn scan_kind(
    catalog: &Catalog,
    kind: Kind,
    symbol: &Arc<str>,
    start: NaiveDate,
    end: NaiveDate,
    hot: Option<LazyFrame>,
) -> PolarsResult<LazyFrame> {
    let files = catalog.files_for_symbol_in_range(symbol, kind, start, end);

    // Every file here already belongs to `symbol` by construction (that's
    // what the lookup was scoped to) -- stamp it as a column so the query
    // text's own `WHERE symbol = '...'` still has something to bind
    // against, cheaply now since it's one literal value over just this
    // symbol's (date-windowed) files, not every symbol's.
    let mut frames: Vec<LazyFrame> = files
        .iter()
        .map(|f| {
            let path = PlRefPath::try_from_path(f)?;
            let lf = LazyFrame::scan_parquet(path, ScanArgsParquet::default())?;
            Ok(lf.with_column(lit(symbol.as_ref()).alias("symbol")))
        })
        .collect::<PolarsResult<Vec<LazyFrame>>>()?;

    // Unconditional: the requested date window scopes the cold files, but
    // never the hot buffer.
    if let Some(hot) = hot {
        frames.push(hot);
    }

    match frames.len() {
        // Nothing cold, nothing hot for this symbol -- a correctly-shaped
        // empty placeholder (not DataFrame::empty(), which has zero
        // columns and would make the query's own WHERE symbol = '...'
        // fail with "column not found" instead of just returning no rows).
        0 => Ok(match kind {
            Kind::Quote => empty_quote_frame(),
            Kind::Trade => empty_trade_frame(),
        }),
        // Exactly one side present: return it directly, no concat (and no
        // risk of a schema mismatch) needed.
        1 => Ok(frames.remove(0)),
        _ => concat(&frames, UnionArgs::default()),
    }
}

/// Runs one query message, scoped to exactly the symbol and date window it
/// names, against that symbol's current data (cold, date-windowed) plus
/// whatever's still buffered hot for it (never date-windowed). Returns the
/// result rendered as CSV text.
///
/// Only `query_type::SQL_CMD` is implemented; anything else (e.g. the
/// as-of join type) is rejected clearly rather than silently mishandled.
///
/// Takes the catalog's read lock only long enough to snapshot the current
/// (date-windowed) file list for `symbol` (`build_context` builds lazy scan
/// plans -- no file I/O happens yet) and releases it before actually
/// executing the query, so a slow query never blocks the batcher/compactor
/// from making progress. The hot buffer lock is held only long enough for
/// `hot_lazyframe_for_symbol` to clone that one entry's builders, for the
/// same reason.
pub fn run_query(
    catalog: &Arc<RwLock<Catalog>>,
    hot_quotes: &SharedQuoteBuffers,
    hot_trades: &SharedTradeBuffers,
    query: &str,
) -> Result<String> {
    let query_info = extract_info(query);

    let query_type = extract_query_type(&query_info);
    if query_type != syntax::query_type::SQL_CMD {
        return Err(anyhow!("query type {query_type:?} is not implemented yet"));
    }

    let symbol = extract_symbol(&query_info)?;
    let sentence = extract_sentence(&query_info)?;

    let start = extract_date(&query_info, syntax::STT_DATE)?;
    let end = extract_date(&query_info, syntax::EDD_DATE)?;
    let today = chrono::Utc::now().date_naive();
    let (range_start, range_end) = resolve_date_range(start, end, today);

    let guard = catalog
        .read()
        .map_err(|e| anyhow!("catalog lock poisoned: {e}"))?;
    let mut ctx = build_context(&guard, hot_quotes, hot_trades, &symbol, range_start, range_end)?;
    drop(guard);

    let mut df = ctx.execute(sentence)?.collect()?;

    let mut out = Vec::new();
    CsvWriter::new(&mut out).finish(&mut df)?;
    Ok(String::from_utf8(out)?)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use ingestion_engine::types::types::{Quote, Trade};
    use tempfile::tempdir;
    use tick_score::hot_buf::{HotBuf, HotBufQuote, HotBufTrade};

    use super::*;

    /// Builds a wire message the way a real client would: `TAG=VALUE`
    /// pairs, each terminated by SOH (0x01), same shape FixParser expects.
    fn build_message(fields: &[(u32, &str)]) -> String {
        fields.iter().map(|(tag, val)| format!("{tag}={val}\u{1}")).collect()
    }

    fn sql_message(symbol: &str, sentence: &str) -> String {
        build_message(&[(syntax::SYMBOL, symbol), (syntax::SENTENCE, sentence)])
    }

    fn sql_message_with_dates(symbol: &str, sentence: &str, start: Option<&str>, end: Option<&str>) -> String {
        let mut fields = vec![(syntax::SYMBOL, symbol), (syntax::SENTENCE, sentence)];
        if let Some(s) = start {
            fields.push((syntax::STT_DATE, s));
        }
        if let Some(e) = end {
            fields.push((syntax::EDD_DATE, e));
        }
        build_message(&fields)
    }

    #[test]
    fn resolve_date_range_defaults_to_today_when_neither_given() {
        let today = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();
        assert_eq!(resolve_date_range(None, None, today), (today, today));
    }

    #[test]
    fn resolve_date_range_extends_five_days_forward_from_a_lone_start() {
        let today = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();
        let start = NaiveDate::from_ymd_opt(2026, 8, 20).unwrap();
        let expected_end = NaiveDate::from_ymd_opt(2026, 8, 24).unwrap();
        assert_eq!(resolve_date_range(Some(start), None, today), (start, expected_end));
    }

    #[test]
    fn resolve_date_range_extends_five_days_backward_from_a_lone_end() {
        let today = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 8, 20).unwrap();
        let expected_start = NaiveDate::from_ymd_opt(2026, 8, 16).unwrap();
        assert_eq!(resolve_date_range(None, Some(end), today), (expected_start, end));
    }

    #[test]
    fn resolve_date_range_uses_both_exactly_as_given_uncapped() {
        let today = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();
        let start = NaiveDate::from_ymd_opt(2026, 8, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 8, 20).unwrap();
        assert_eq!(resolve_date_range(Some(start), Some(end), today), (start, end));
    }

    #[test]
    fn resolve_date_range_tolerates_start_and_end_given_in_reverse_order() {
        let today = NaiveDate::from_ymd_opt(2026, 9, 2).unwrap();
        let earlier = NaiveDate::from_ymd_opt(2026, 8, 1).unwrap();
        let later = NaiveDate::from_ymd_opt(2026, 8, 20).unwrap();
        assert_eq!(resolve_date_range(Some(later), Some(earlier), today), (earlier, later));
    }

    fn empty_quotes() -> SharedQuoteBuffers {
        Arc::new(RwLock::new(HashMap::new()))
    }

    fn empty_trades() -> SharedTradeBuffers {
        Arc::new(RwLock::new(HashMap::new()))
    }

    fn write_test_parquet(path: &std::path::Path, symbols: &[&str], prices: &[f64]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut df = DataFrame::new(
            symbols.len(),
            vec![
                Series::new("symbol".into(), symbols).into(),
                Series::new("price".into(), prices).into(),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(path).unwrap();
        polars::io::parquet::write::ParquetWriter::new(file)
            .finish(&mut df)
            .unwrap();
    }

    #[test]
    fn run_query_on_empty_catalog_and_empty_hot_buffers_returns_zero_rows_not_an_error() {
        let catalog = Arc::new(RwLock::new(Catalog::new(std::path::PathBuf::from("unused"))));

        let csv = run_query(
            &catalog,
            &empty_quotes(),
            &empty_trades(),
            &sql_message("BTC-USD", "SELECT * FROM trades WHERE symbol = 'BTC-USD'"),
        )
        .unwrap();

        // a real (correctly-shaped) table with zero rows, not a query error
        let mut lines = csv.lines();
        assert_eq!(lines.next(), Some("receive_time,trade_px,trade_vol,trd_type,symbol"));
        assert_eq!(lines.next(), None);
    }

    #[test]
    fn run_query_rejects_a_message_with_no_symbol() {
        let catalog = Arc::new(RwLock::new(Catalog::new(std::path::PathBuf::from("unused"))));
        let message = build_message(&[(syntax::SENTENCE, "SELECT * FROM trades")]);

        let result = run_query(&catalog, &empty_quotes(), &empty_trades(), &message);

        let err = result.expect_err("a message with no symbol field should be rejected");
        assert!(err.to_string().contains("must specify a symbol"), "got: {err}");
    }

    #[test]
    fn run_query_rejects_a_message_with_no_sentence() {
        let catalog = Arc::new(RwLock::new(Catalog::new(std::path::PathBuf::from("unused"))));
        let message = build_message(&[(syntax::SYMBOL, "BTC-USD")]);

        let result = run_query(&catalog, &empty_quotes(), &empty_trades(), &message);

        let err = result.expect_err("a message with no sentence field should be rejected");
        assert!(err.to_string().contains("SQL sentence"), "got: {err}");
    }

    #[test]
    fn run_query_rejects_an_unimplemented_query_type() {
        let catalog = Arc::new(RwLock::new(Catalog::new(std::path::PathBuf::from("unused"))));
        let message = build_message(&[
            (syntax::QUERY_TYPE, syntax::query_type::JOIN_AS_OF),
            (syntax::SYMBOL, "BTC-USD"),
            (syntax::SENTENCE, "SELECT * FROM trades"),
        ]);

        let result = run_query(&catalog, &empty_quotes(), &empty_trades(), &message);

        let err = result.expect_err("JOIN_AS_OF isn't implemented yet and should be rejected clearly");
        assert!(err.to_string().contains("not implemented"), "got: {err}");
    }

    #[test]
    fn run_query_rejects_a_malformed_date() {
        let catalog = Arc::new(RwLock::new(Catalog::new(std::path::PathBuf::from("unused"))));
        let message = sql_message_with_dates("BTC-USD", "SELECT * FROM trades", Some("not-a-date"), None);

        let result = run_query(&catalog, &empty_quotes(), &empty_trades(), &message);

        assert!(result.is_err());
    }

    #[test]
    fn run_query_filters_by_symbol_across_multiple_files() {
        let dir = tempdir().unwrap();
        let mut catalog = Catalog::new(dir.path().to_path_buf());
        let date = chrono::Utc::now().date_naive();

        // One file per symbol -- matching how the real system actually
        // flushes (one HotBuf, one symbol, one file), not one file holding
        // rows from several symbols at once.
        let f1 = dir.path().join("btc.parquet");
        let f2 = dir.path().join("eth.parquet");
        write_test_parquet(&f1, &["BTC-USD"], &[100.0]);
        write_test_parquet(&f2, &["ETH-USD"], &[50.0]);
        catalog.add_file(Arc::from("BTC-USD"), Kind::Trade, date, f1);
        catalog.add_file(Arc::from("ETH-USD"), Kind::Trade, date, f2);

        let catalog = Arc::new(RwLock::new(catalog));

        let csv = run_query(
            &catalog,
            &empty_quotes(),
            &empty_trades(),
            &sql_message("BTC-USD", "SELECT * FROM trades WHERE symbol = 'BTC-USD'"),
        )
        .unwrap();

        let mut lines = csv.lines();
        assert_eq!(lines.next(), Some("symbol,price"));
        assert_eq!(lines.next(), Some("BTC-USD,100.0"), "only the filtered symbol's row");
        assert_eq!(lines.next(), None, "ETH-USD's row must not appear");
    }

    #[test]
    fn run_query_excludes_files_outside_the_resolved_date_window() {
        let dir = tempdir().unwrap();
        let mut catalog = Catalog::new(dir.path().to_path_buf());

        let in_window = NaiveDate::from_ymd_opt(2026, 8, 20).unwrap();
        let outside_window = NaiveDate::from_ymd_opt(2026, 8, 1).unwrap();

        let f_in = dir.path().join("in_window.parquet");
        let f_out = dir.path().join("outside_window.parquet");
        write_test_parquet(&f_in, &["BTC-USD"], &[100.0]);
        write_test_parquet(&f_out, &["BTC-USD"], &[1.0]);
        catalog.add_file(Arc::from("BTC-USD"), Kind::Trade, in_window, f_in);
        catalog.add_file(Arc::from("BTC-USD"), Kind::Trade, outside_window, f_out);

        let catalog = Arc::new(RwLock::new(catalog));

        // explicit end_date=2026-08-20, no start_date -> window is
        // 2026-08-16..=2026-08-20, which includes in_window but not
        // outside_window (2026-08-01).
        let message = sql_message_with_dates(
            "BTC-USD",
            "SELECT * FROM trades WHERE symbol = 'BTC-USD'",
            None,
            Some("2026-08-20"),
        );

        let csv = run_query(&catalog, &empty_quotes(), &empty_trades(), &message).unwrap();

        let mut lines = csv.lines();
        assert_eq!(lines.next(), Some("symbol,price"));
        assert_eq!(lines.next(), Some("BTC-USD,100.0"), "the in-window file's row");
        assert_eq!(lines.next(), None, "the outside-window file's row must not appear");
    }

    #[test]
    fn run_query_propagates_invalid_sql_as_an_error() {
        let catalog = Arc::new(RwLock::new(Catalog::new(std::path::PathBuf::from("unused"))));

        let result = run_query(
            &catalog,
            &empty_quotes(),
            &empty_trades(),
            &sql_message("BTC-USD", "not even sql WHERE symbol = 'BTC-USD'"),
        );

        assert!(result.is_err());
    }

    #[test]
    fn run_query_sees_hot_data_that_has_never_been_flushed() {
        // empty catalog: nothing has ever been written to disk
        let catalog = Arc::new(RwLock::new(Catalog::new(std::path::PathBuf::from("unused"))));

        let mut hot_buf = HotBufTrade::new(4);
        hot_buf.push(Trade {
            receive_time: chrono::Utc::now(),
            ticker_name: Arc::from("BTC-USD"),
            trade_px: 100.0,
            trade_vol: 5,
            trd_type: Some(1),
        });
        let hot_trades: SharedTradeBuffers = Arc::new(RwLock::new(HashMap::from([(
            Arc::from("BTC-USD"),
            (hot_buf, 0u32),
        )])));

        let csv = run_query(
            &catalog,
            &empty_quotes(),
            &hot_trades,
            &sql_message("BTC-USD", "SELECT * FROM trades WHERE symbol = 'BTC-USD'"),
        )
        .unwrap();

        assert!(csv.contains("100.0"), "expected the unflushed hot row, got: {csv:?}");
        assert!(csv.contains("BTC-USD"), "the hot side's stamped symbol column, got: {csv:?}");
    }

    #[test]
    fn run_query_sees_hot_data_even_when_the_date_window_is_entirely_historical() {
        // "keep the current hot buffer": a query asking only about old
        // dates must still see today's not-yet-flushed rows.
        let catalog = Arc::new(RwLock::new(Catalog::new(std::path::PathBuf::from("unused"))));

        let mut hot_buf = HotBufTrade::new(4);
        hot_buf.push(Trade {
            receive_time: chrono::Utc::now(),
            ticker_name: Arc::from("BTC-USD"),
            trade_px: 100.0,
            trade_vol: 5,
            trd_type: Some(1),
        });
        let hot_trades: SharedTradeBuffers = Arc::new(RwLock::new(HashMap::from([(
            Arc::from("BTC-USD"),
            (hot_buf, 0u32),
        )])));

        let message = sql_message_with_dates(
            "BTC-USD",
            "SELECT * FROM trades WHERE symbol = 'BTC-USD'",
            Some("2020-01-01"),
            None,
        );

        let csv = run_query(&catalog, &empty_quotes(), &hot_trades, &message).unwrap();

        assert!(csv.contains("100.0"), "hot row must still be visible, got: {csv:?}");
    }

    #[test]
    fn run_query_unions_cold_and_hot_rows_for_the_same_symbol() {
        let dir = tempdir().unwrap();
        let mut catalog = Catalog::new(dir.path().to_path_buf());
        let date = chrono::Utc::now().date_naive();

        // The cold row: built via HotBufQuote so its parquet schema matches
        // the hot side exactly (receive_time, bid_price, bid_vol, ask_price,
        // ask_vol) rather than hand-rolling a schema that would mismatch
        // what concat() actually needs to union.
        let mut cold_buf = HotBufQuote::new(4);
        cold_buf.push(Quote {
            receive_time: chrono::Utc::now(),
            ticker_name: Arc::from("BTC-USD"),
            bid_price: 50.0,
            bid_vol: 10,
            ask_price: 50.5,
            ask_vol: 12,
        });
        let mut cold_df = cold_buf.to_dataframe();
        let cold_file = dir.path().join("cold.parquet");
        std::fs::create_dir_all(cold_file.parent().unwrap()).unwrap();
        let file = std::fs::File::create(&cold_file).unwrap();
        polars::io::parquet::write::ParquetWriter::new(file)
            .finish(&mut cold_df)
            .unwrap();
        catalog.add_file(Arc::from("BTC-USD"), Kind::Quote, date, cold_file);
        let catalog = Arc::new(RwLock::new(catalog));

        // The hot row: same symbol, still buffered, never flushed.
        let mut hot_buf = HotBufQuote::new(4);
        hot_buf.push(Quote {
            receive_time: chrono::Utc::now(),
            ticker_name: Arc::from("BTC-USD"),
            bid_price: 100.0,
            bid_vol: 10,
            ask_price: 100.5,
            ask_vol: 12,
        });
        let hot_quotes: SharedQuoteBuffers = Arc::new(RwLock::new(HashMap::from([(
            Arc::from("BTC-USD"),
            (hot_buf, 0u32),
        )])));

        let csv = run_query(
            &catalog,
            &hot_quotes,
            &empty_trades(),
            &sql_message("BTC-USD", "SELECT bid_price FROM quotes WHERE symbol = 'BTC-USD' ORDER BY bid_price"),
        )
        .unwrap();

        let mut lines = csv.lines();
        assert_eq!(lines.next(), Some("bid_price"));
        assert_eq!(lines.next(), Some("50.0"), "the cold (flushed) row");
        assert_eq!(lines.next(), Some("100.0"), "the hot (unflushed) row");
        assert_eq!(lines.next(), None);
    }
}
