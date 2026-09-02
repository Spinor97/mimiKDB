use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use ingestion_engine::types::types::{Quote, Trade};
use polars::chunked_array::builder::PrimitiveChunkedBuilder;
use polars::datatypes::{Float32Type, Int64Type, UInt32Type, UInt8Type};
use polars::prelude::*;

/// The batcher's per-symbol hot buffer for one kind, shared so a live
/// query can read it -- see [`hot_lazyframe_for_symbol`].
pub type SharedQuoteBuffers = Arc<RwLock<HashMap<Arc<str>, (HotBufQuote, u32)>>>;
pub type SharedTradeBuffers = Arc<RwLock<HashMap<Arc<str>, (HotBufTrade, u32)>>>;

/// Shared read/flush surface for the per-symbol hot buffers. `push` stays an
/// inherent method on each concrete type (`Quote` vs `Trade` take different
/// arguments), but everything the batcher's flush-check sweep needs --
/// "how many rows", "how stale is the oldest one", "hand me a snapshot",
/// "clear on successful flush" -- is the same shape for both, so it's
/// written once here instead of duplicated per kind.
pub trait HotBuf {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// `receive_time` of the oldest buffered row, if any.
    fn oldest_receive_time(&self) -> Option<DateTime<Utc>>;
    /// Consumes the buffered rows into a DataFrame. Takes `self` by value
    /// (PrimitiveChunkedBuilder::finish() consumes) -- call this on the
    /// value `take()` hands you, not on the live buffer, or you'll lose
    /// whatever arrives next while this snapshot is still being written.
    fn to_dataframe(self) -> DataFrame;
    /// Non-destructive read of the currently buffered rows -- for a live
    /// query, which must never disturb what's still accumulating. Clones
    /// each column builder before finishing the clone (PrimitiveChunkedBuilder
    /// derives Clone; finish() only consumes the clone), leaving the live
    /// buffer untouched. Costs a copy on every call, unlike `take()` --
    /// acceptable for a query, wrong for the flush path.
    fn snapshot(&self) -> DataFrame;
    /// Called by the batcher only after a snapshot has been durably flushed.
    fn clear(&mut self);
    /// Swaps in a fresh, empty buffer and returns the old one (owned), so
    /// the caller can `to_dataframe()` it while whatever arrives next
    /// accumulates in the replacement -- this is the flush-time "read and
    /// reset" step; each impl knows its own capacity, so it lives here
    /// rather than at the call site.
    fn take(&mut self) -> Self
    where
        Self: Sized;
}

pub struct HotBufQuote {
    times: PrimitiveChunkedBuilder<Int64Type>,
    bid_prices: PrimitiveChunkedBuilder<Float32Type>,
    bid_vols: PrimitiveChunkedBuilder<UInt32Type>,
    ask_prices: PrimitiveChunkedBuilder<Float32Type>,
    ask_vols: PrimitiveChunkedBuilder<UInt32Type>,
    // PrimitiveChunkedBuilder exposes no len()/peek() of its own (its inner
    // array is a private field of the polars-core type), so these are
    // tracked alongside the builders instead of read back out of them.
    len: usize,
    oldest_receive_time: Option<DateTime<Utc>>,
    capacity: usize,
}

impl HotBufQuote {
    pub fn new(capacity: usize) -> Self {
        Self {
            times: PrimitiveChunkedBuilder::new("receive_time".into(), capacity),
            bid_prices: PrimitiveChunkedBuilder::new("bid_price".into(), capacity),
            bid_vols: PrimitiveChunkedBuilder::new("bid_vol".into(), capacity),
            ask_prices: PrimitiveChunkedBuilder::new("ask_price".into(), capacity),
            ask_vols: PrimitiveChunkedBuilder::new("ask_vol".into(), capacity),
            len: 0,
            oldest_receive_time: None,
            capacity,
        }
    }

    pub fn push(&mut self, quote: Quote) {
        if self.oldest_receive_time.is_none() {
            self.oldest_receive_time = Some(quote.receive_time);
        }
        self.times.append_value(quote.receive_time.timestamp_micros());
        self.bid_prices.append_value(quote.bid_price);
        self.bid_vols.append_value(quote.bid_vol);
        self.ask_prices.append_value(quote.ask_price);
        self.ask_vols.append_value(quote.ask_vol);
        self.len += 1;
    }

    fn build_dataframe(
        len: usize,
        times: PrimitiveChunkedBuilder<Int64Type>,
        bid_prices: PrimitiveChunkedBuilder<Float32Type>,
        bid_vols: PrimitiveChunkedBuilder<UInt32Type>,
        ask_prices: PrimitiveChunkedBuilder<Float32Type>,
        ask_vols: PrimitiveChunkedBuilder<UInt32Type>,
    ) -> DataFrame {
        DataFrame::new(len, vec![
            times.finish().into_datetime(TimeUnit::Microseconds, None).into_series().into(),
            bid_prices.finish().into_series().into(),
            bid_vols.finish().into_series().into(),
            ask_prices.finish().into_series().into(),
            ask_vols.finish().into_series().into(),
        ])
        .expect("fixed set of equal-length columns")
    }
}

impl HotBuf for HotBufQuote {
    fn len(&self) -> usize {
        self.len
    }

    fn oldest_receive_time(&self) -> Option<DateTime<Utc>> {
        self.oldest_receive_time
    }

    fn to_dataframe(self) -> DataFrame {
        // Owns self already -- finish the builders directly, no clone needed.
        Self::build_dataframe(
            self.len,
            self.times,
            self.bid_prices,
            self.bid_vols,
            self.ask_prices,
            self.ask_vols,
        )
    }

    fn snapshot(&self) -> DataFrame {
        // PrimitiveChunkedBuilder derives Clone; finish() consumes, so
        // finishing a clone leaves the live builder untouched for the
        // batcher to keep appending to.
        Self::build_dataframe(
            self.len,
            self.times.clone(),
            self.bid_prices.clone(),
            self.bid_vols.clone(),
            self.ask_prices.clone(),
            self.ask_vols.clone(),
        )
    }

    fn clear(&mut self) {
        *self = HotBufQuote::new(self.capacity);
    }

    fn take(&mut self) -> Self {
        std::mem::replace(self, HotBufQuote::new(self.capacity))
    }
}

pub struct HotBufTrade {
    times: PrimitiveChunkedBuilder<Int64Type>,
    trade_pxs: PrimitiveChunkedBuilder<Float32Type>,
    trade_vols: PrimitiveChunkedBuilder<UInt32Type>,
    trd_types: PrimitiveChunkedBuilder<UInt8Type>,
    len: usize,
    oldest_receive_time: Option<DateTime<Utc>>,
    capacity: usize,
}

impl HotBufTrade {
    pub fn new(capacity: usize) -> Self {
        Self {
            times: PrimitiveChunkedBuilder::new("receive_time".into(), capacity),
            trade_pxs: PrimitiveChunkedBuilder::new("trade_px".into(), capacity),
            trade_vols: PrimitiveChunkedBuilder::new("trade_vol".into(), capacity),
            trd_types: PrimitiveChunkedBuilder::new("trd_type".into(), capacity),
            len: 0,
            oldest_receive_time: None,
            capacity,
        }
    }

    pub fn push(&mut self, trade: Trade) {
        if self.oldest_receive_time.is_none() {
            self.oldest_receive_time = Some(trade.receive_time);
        }
        self.times.append_value(trade.receive_time.timestamp_micros());
        self.trade_pxs.append_value(trade.trade_px);
        self.trade_vols.append_value(trade.trade_vol);
        self.trd_types.append_option(trade.trd_type);
        self.len += 1;
    }

    fn build_dataframe(
        len: usize,
        times: PrimitiveChunkedBuilder<Int64Type>,
        trade_pxs: PrimitiveChunkedBuilder<Float32Type>,
        trade_vols: PrimitiveChunkedBuilder<UInt32Type>,
        trd_types: PrimitiveChunkedBuilder<UInt8Type>,
    ) -> DataFrame {
        DataFrame::new(len, vec![
            times.finish().into_datetime(TimeUnit::Microseconds, None).into_series().into(),
            trade_pxs.finish().into_series().into(),
            trade_vols.finish().into_series().into(),
            trd_types.finish().into_series().into(),
        ])
        .expect("fixed set of equal-length columns")
    }
}

impl HotBuf for HotBufTrade {
    fn len(&self) -> usize {
        self.len
    }

    fn oldest_receive_time(&self) -> Option<DateTime<Utc>> {
        self.oldest_receive_time
    }

    fn to_dataframe(self) -> DataFrame {
        Self::build_dataframe(self.len, self.times, self.trade_pxs, self.trade_vols, self.trd_types)
    }

    fn snapshot(&self) -> DataFrame {
        Self::build_dataframe(
            self.len,
            self.times.clone(),
            self.trade_pxs.clone(),
            self.trade_vols.clone(),
            self.trd_types.clone(),
        )
    }

    fn clear(&mut self) {
        *self = HotBufTrade::new(self.capacity);
    }

    fn take(&mut self) -> Self {
        std::mem::replace(self, HotBufTrade::new(self.capacity))
    }
}

/// Builds a LazyFrame for exactly one symbol's currently buffered rows, or
/// `None` if that symbol isn't buffered (or is buffered but empty) -- the
/// "hot" half of a live, symbol-scoped hot ∪ cold query. A single map
/// lookup, not an iteration over every symbol: a query for one symbol never
/// touches any other symbol's hot data. Non-destructive: snapshots the
/// buffer without disturbing what the batcher keeps appending to it.
///
/// Stamps a `symbol` column onto the snapshot using the caller-supplied
/// symbol (the same one used to look it up) -- `HotBufQuote`/`HotBufTrade`
/// don't carry their own symbol (it's the `HashMap` key, not a per-row
/// field), so this is the one place that actually knows it.
pub fn hot_lazyframe_for_symbol<B: HotBuf>(
    buffers: &Arc<RwLock<HashMap<Arc<str>, (B, u32)>>>,
    symbol: &Arc<str>,
) -> PolarsResult<Option<LazyFrame>> {
    let guard = buffers
        .read()
        .map_err(|e| PolarsError::ComputeError(format!("hot buffer lock poisoned: {e}").into()))?;

    let buf = if *&symbol.deref().is_empty() {
        let rtn: Vec<LazyFrame> = guard.iter().map(|(_, (buf, _))| buf.snapshot().lazy().with_column(lit(symbol.as_ref()).alias("symbol"))).collect();
        if rtn.is_empty() {
            return Ok(None);
        }
        concat(&rtn,UnionArgs::default())?.sort(["receive_time"], SortMultipleOptions::default())
    } else{
        let Some((buf, _)) = guard.get(symbol) else {
            return Ok(None);
        };
        if buf.is_empty() {
            return Ok(None);
        }
        buf.snapshot().lazy().with_column(lit(symbol.as_ref()).alias("symbol"))
    };
    
    Ok(Some(buf))
}

/// A zero-row, correctly-shaped-and-named `LazyFrame` for a kind with
/// truly nothing to show yet -- no cold files, nothing hot either. Used as
/// a query's placeholder in that case so a query that references any real
/// column (including `symbol`, stamped the same way `hot_lazyframe` stamps
/// it) still returns an empty result instead of "column not found": an
/// all-zero-column `DataFrame::empty()` doesn't know about that column at
/// all, but this does, because it's built the same way a real (empty)
/// buffer would be.
pub fn empty_quote_frame() -> LazyFrame {
    HotBufQuote::new(0)
        .snapshot()
        .lazy()
        .with_column(lit(NULL).cast(DataType::String).alias("symbol"))
}

pub fn empty_trade_frame() -> LazyFrame {
    HotBufTrade::new(0)
        .snapshot()
        .lazy()
        .with_column(lit(NULL).cast(DataType::String).alias("symbol"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn sample_quote(receive_time: DateTime<Utc>, bid_price: f32, ask_price: f32) -> Quote {
        Quote {
            receive_time,
            ticker_name: Arc::from("BTC-USD"),
            bid_price,
            bid_vol: 10,
            ask_price,
            ask_vol: 12,
        }
    }

    fn sample_trade(receive_time: DateTime<Utc>, trade_px: f32) -> Trade {
        Trade {
            receive_time,
            ticker_name: Arc::from("BTC-USD"),
            trade_px,
            trade_vol: 5,
            trd_type: Some(1),
        }
    }

    #[test]
    fn quote_buf_starts_empty() {
        let buf = HotBufQuote::new(4);
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
        assert_eq!(buf.oldest_receive_time(), None);
    }

    #[test]
    fn quote_buf_tracks_len_and_the_first_rows_own_receive_time() {
        let mut buf = HotBufQuote::new(4);
        let t1 = Utc::now();
        let t2 = t1 + Duration::seconds(1);

        buf.push(sample_quote(t1, 100.0, 100.5));
        buf.push(sample_quote(t2, 101.0, 101.5));

        assert_eq!(buf.len(), 2);
        assert!(!buf.is_empty());
        // oldest_receive_time must be the first row's own receive_time, not
        // whatever wall-clock time happened to be when push() ran, and must
        // stay pinned to the first row even after a second push.
        assert_eq!(buf.oldest_receive_time(), Some(t1));
    }

    #[test]
    fn quote_buf_to_dataframe_has_expected_columns_and_values() {
        let mut buf = HotBufQuote::new(4);
        let t = Utc::now();
        buf.push(sample_quote(t, 100.0, 100.5));
        buf.push(sample_quote(t, 101.0, 101.5));

        let df = buf.to_dataframe();

        assert_eq!(df.height(), 2);
        assert_eq!(
            df.get_column_names()
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>(),
            vec!["receive_time", "bid_price", "bid_vol", "ask_price", "ask_vol"]
        );

        let bid_prices: Vec<Option<f32>> = df
            .column("bid_price")
            .unwrap()
            .f32()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(bid_prices, vec![Some(100.0), Some(101.0)]);

        let ask_prices: Vec<Option<f32>> = df
            .column("ask_price")
            .unwrap()
            .f32()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(ask_prices, vec![Some(100.5), Some(101.5)]);
    }

    #[test]
    fn quote_buf_take_hands_back_old_data_and_resets_in_place() {
        let mut buf = HotBufQuote::new(4);
        buf.push(sample_quote(Utc::now(), 100.0, 100.5));
        buf.push(sample_quote(Utc::now(), 101.0, 101.5));

        let taken = buf.take();

        // the live buffer is immediately usable again, empty
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
        assert_eq!(buf.oldest_receive_time(), None);
        buf.push(sample_quote(Utc::now(), 102.0, 102.5));
        assert_eq!(buf.len(), 1);

        // the taken value still has everything that was pushed before take()
        assert_eq!(taken.len(), 2);
        assert_eq!(taken.to_dataframe().height(), 2);
    }

    #[test]
    fn quote_buf_clear_resets_state() {
        let mut buf = HotBufQuote::new(4);
        buf.push(sample_quote(Utc::now(), 100.0, 100.5));

        buf.clear();

        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
        assert_eq!(buf.oldest_receive_time(), None);
    }

    #[test]
    fn trade_buf_tracks_len_and_oldest_receive_time() {
        let mut buf = HotBufTrade::new(4);
        let t1 = Utc::now();
        let t2 = t1 + Duration::seconds(1);

        buf.push(sample_trade(t1, 100.0));
        buf.push(sample_trade(t2, 101.0));

        assert_eq!(buf.len(), 2);
        assert_eq!(buf.oldest_receive_time(), Some(t1));
    }

    #[test]
    fn trade_buf_to_dataframe_has_expected_columns_and_values() {
        let mut buf = HotBufTrade::new(4);
        let t = Utc::now();
        buf.push(sample_trade(t, 100.0));
        buf.push(sample_trade(t, 101.0));

        let df = buf.to_dataframe();

        assert_eq!(df.height(), 2);
        assert_eq!(
            df.get_column_names()
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>(),
            vec!["receive_time", "trade_px", "trade_vol", "trd_type"]
        );

        let pxs: Vec<Option<f32>> = df
            .column("trade_px")
            .unwrap()
            .f32()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(pxs, vec![Some(100.0), Some(101.0)]);
    }

    #[test]
    fn trade_buf_take_hands_back_old_data_and_resets_in_place() {
        let mut buf = HotBufTrade::new(4);
        buf.push(sample_trade(Utc::now(), 100.0));

        let taken = buf.take();

        assert_eq!(buf.len(), 0);
        assert_eq!(taken.len(), 1);
        assert_eq!(taken.to_dataframe().height(), 1);
    }
}
