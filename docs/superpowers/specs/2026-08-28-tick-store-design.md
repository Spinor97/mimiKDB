# tick_store: batched parquet persistence + live query + as-of join for FIX ticks

## Revision note (2026-08-31)

This spec was revised after the original DataFusion-based query design turned
out to be a poor fit for as-of join: DataFusion has no native ASOF JOIN, and
the original "one immutable parquet file per flush, forever" write pattern
means an as-of join would have to stitch together many small files, which is
slow (per-file footer/decompression/IO overhead adds up). This revision:

- Replaces DataFusion with **Polars** (which has a native, optimized
  `join_asof`) as the query engine, for both ad-hoc SQL and the as-of join.
- Adds a **compactor** that periodically merges a partition's small files
  into fewer, larger ones, and a **file catalog** that gives every reader
  (query, as-of join, and the compactor itself) a consistent, race-free view
  of "what files currently represent this partition" — this walks back the
  original "no compaction" non-goal, but keeps it tightly scoped
  (single-process, no cross-partition retention policy).
- Adds `asof_join`, a dedicated function (not a SQL query) for the
  trade-matched-to-latest-quote join pattern.

Everything under "Context" through "Prerequisite" is unchanged from the
original design and already substantially implemented (`symbol` as
`Arc<str>`, interned via `SymbolInitilizer`, threaded through `dispatch()`).
"Data flow" through "Testing" reflect the revised design.

## Context

`ingestion_engine::network::session::run` decodes a FIX market-data session
into a stream of `Tick` values (`Quote` or `Trade`, see
`lib/ingestion_engine/src/types/types.rs`), delivered as a `Stream<Item =
Result<Tick>>` wrapping a channel. Today that's the end of the pipeline —
nothing persists ticks or lets anything query them.

This spec covers a new subsystem that:

1. Consumes that `Tick` stream reactively (Rx-style: the receiver treated as
   a `futures::Stream` and composed with combinators).
2. Batches ticks per symbol and flushes each batch to a local parquet file
   once "a few rows" have accumulated (count-based), with a time-based
   backstop so a quiet symbol's ticks still land on disk promptly.
3. Periodically compacts a partition's accumulated small files into fewer,
   larger ones, so reads later don't pay small-file overhead.
4. Exposes a live SQL query interface (via Polars) that transparently spans
   both data already flushed to parquet and data still sitting in the
   in-memory batch waiting to be flushed.
5. Exposes an `asof_join` function: for each trade, the latest quote at or
   before it, same symbol — the standard finance tick-matching query,
   implemented as a Polars `join_asof` over the same hot+cold data.

## Non-goals

- No network-facing query server (no socket/IPC protocol). The query
  surface is an in-process async function, callable only by code linked
  into the same binary. A server can be layered on top later without
  changing the core.
- No cross-process coordination, replication, or historical backfill.
  Single writer process, single directory tree.
- Compaction is in scope, but only as "keep per-partition file count small
  so scans stay cheap" — no retention/expiry policy, no cross-partition
  scheduling, no multi-process coordination of compaction.
- No change to FIX session/network/codec behavior beyond adding `symbol` to
  the decoded tick types (already implemented).

## Crate boundary

New crate `lib/tick_store`, alongside the existing `ingestion_engine`:

```
lib/
  ingestion_engine/   # unchanged scope: FIX session -> Stream<Item = Result<Tick>>
  tick_store/         # new
    Cargo.toml        # depends on ingestion_engine (for Tick/Quote/Trade),
                       # polars (lazy, parquet, sql features), tokio, tokio-stream
    src/
      lib.rs
      hot_buffer.rs   # shared in-memory state: rows accumulated but not yet flushed
      batcher.rs      # consumes the Tick stream, groups by (kind, symbol), flushes
      catalog.rs       # FileCatalog: current file list per partition
      writer.rs       # Tick rows -> parquet file, partitioned path layout, updates catalog
      compactor.rs     # periodic merge of small files per partition, updates catalog
      query.rs        # Polars SQLContext + asof_join, built from catalog + hot buffer
      config.rs       # BatchConfig { max_rows, max_age, data_dir, compact_interval, compact_threshold }
fook_kdb/
  src/main.rs         # wires session::run() -> tick_store::start(...), then
                       # calls tick_store::query(sql) / tick_store::asof_join(...) as needed
```

`ingestion_engine` gains no new dependencies. `tick_store` is independently
testable: its tests construct `Tick` values directly and never touch
FIX/network code.

## Prerequisite: `Tick`/`Quote`/`Trade` carry an interned `symbol`

**Already implemented.** `Quote`/`Trade` carry `ticker_name: Arc<str>`,
resolved in `session::dispatch()` via a `SymbolInitilizer` (a small
`HashMap<String, Arc<str>>` cache) before `Tick::from_raw` is called, so
every tick for the same symbol shares one `Arc` allocation rather than
re-copying the string per tick.

## Data flow

```
session::run(...)                                   tick_store
  -> Stream<Item = Result<Tick>>
       │
       ▼
  Batcher
    - HashMap<(Kind, Symbol), Vec<Tick>>   <-- this *is* the hot buffer,
                                                shared via Arc<RwLock<_>>
    - flush a key when:
        len(key) >= max_rows        (count trigger)
        OR age(oldest row) >= max_age   (time backstop)
       │  on flush: (kind, symbol, Vec<Tick>), then clear that key
       ▼
  writer::flush(kind, symbol, rows)
    -> parquet file under
       data/{trades|quotes}/symbol={symbol}/date={YYYY-MM-DD}/part-{n}.parquet
    -> FileCatalog: append this path to the partition's file list

  (independently, on a timer)
  Compactor
    -> FileCatalog: find partitions with > compact_threshold closed files
    -> merge them (already time-ordered, so a plain concatenation) into one
       part-merged-{n}.parquet
    -> FileCatalog: atomically swap the partition's file list to [merged path]
    -> delete the old small files
```

### Batcher

Unchanged from the original design: an async task shaped like the existing
`session.rs` read loop (`tokio::select!` over "next tick" vs. "flush-check
interval ticked"), not a chain of `Stream` combinators — grouping by symbol
with independent per-key deadlines doesn't fit `.chunks()`/`.timeout()`
cleanly.

### File catalog

```rust
pub struct FileCatalog {
    partitions: HashMap<(Kind, Arc<str>, NaiveDate), Vec<PathBuf>>,
}
```

Shared via `Arc<RwLock<FileCatalog>>` between the writer, the compactor, and
every read path (`query`, `asof_join`). This is the single source of truth
for "what files currently represent this partition":

- The **writer** appends a path after each successful flush. Because a
  partition only ever has one writer (the batcher, single-threaded per
  key), the `Vec` is always in time order — reading the files in that order
  is equivalent to reading one continuous sorted stream, with no per-file
  sorting needed.
- The **compactor** replaces a partition's `Vec` with `vec![merged_path]` in
  one write-lock swap, only after the merged file is fully written and
  `fsync`'d. A reader taking a read-lock at any point sees either the old
  small-files list or the new single-file list — never a half-updated one,
  and never a path that's been deleted out from under it.
- This also removes the need for a raw directory listing (and its
  read/delete race) anywhere in `tick_store` — both `query` and `asof_join`
  build their Polars `LazyFrame`s directly from a catalog snapshot.

### Writer

Unchanged from the original design (one new immutable file per flush,
Hive-style `symbol=`/`date=` partition path, in-process file-numbering
counter, write failure keeps rows in the hot buffer for retry) — with one
addition: on success, it appends the new file's path to the `FileCatalog`
entry for that partition.

### Compactor

A periodic background task (`tokio::interval`, e.g. every 5 minutes,
configurable):

- Scans the catalog for partitions with more files than
  `compact_threshold` (e.g. 5).
- For each such partition, **excluding the single most-recently-added
  file** (the safety margin that keeps the compactor from ever racing the
  writer for the same partition — it only ever touches files the writer
  has already finished with): read each file's rows in catalog order
  (already time-ordered, so this is a concatenation), write one new
  `part-merged-{n}.parquet`, `fsync` it.
- Atomically swap the catalog entry to `[merged_path, <the excluded most
  recent file>]`, then delete the old small files.
- On failure (disk error, etc.): log and skip: the small files are left
  as-is (still fully correct, just not yet optimized) and retried on the
  next timer tick. The old files are never deleted before the merged file
  is confirmed durable.

This is the part of the design that directly answers the original
motivation: without it, an active symbol accumulates one small file per
flush indefinitely, and both `query` and `asof_join` would have to read
through all of them every time — compaction keeps that count bounded.

### Query and `asof_join` (Polars)

```rust
pub async fn query(sql: &str) -> Result<DataFrame>;
pub async fn asof_join(symbol: &str, range: TimeRange) -> Result<DataFrame>;
```

Both are built from the same per-`(kind, symbol)` assembly:

```rust
let lazy = concat(
    catalog.files_for(kind, symbol, date_range)
        .map(|f| LazyFrame::scan_parquet(f))
        .chain(std::iter::once(hot_buffer.as_lazyframe(kind, symbol)))
)?;
```

Cold files first (already time-ordered via the catalog), hot buffer last
(always the most recent) — the concatenation is the entire "hot ∪ cold,
correctly ordered" story; no sort is computed at query time, because the
ordering is a structural guarantee from how files are written, not
something derived per-query.

- **`query(sql)`**: register the `trades`/`quotes` `LazyFrame`s under those
  names in a `polars::sql::SQLContext`, run `sql`, return the resulting
  `DataFrame`.
- **`asof_join(symbol, range)`**: filter both sides to `symbol`/`range`,
  mark both `.set_sorted("receive_time")` — asserting what the catalog
  already guarantees, so Polars' `join_asof` takes its fast sorted-merge
  path instead of re-sorting or defensively re-checking — then
  `.join_asof(other, left_on="receive_time", right_on="receive_time",
  strategy=Backward).collect()`. Returns the joined `DataFrame` directly
  (no conversion back to `Trade`/`Quote` structs — callers that want to do
  more with the result stay in Polars).

**Known gap (unchanged from the original design):** a row is briefly
invisible between being cleared from the hot buffer and its parquet file
finishing the write (never double-counted, occasionally momentarily
missing). Narrow window, given batches are small and flushes fast — worth
naming, acceptable for this design.

### Schema

Unchanged:

```
trades: receive_time, symbol, trade_px, trade_vol, trd_type
quotes: receive_time, symbol, bid_price, bid_vol, ask_price, ask_vol
```

`symbol` is both a row column and a partition key (Hive-style); the parquet
file itself does not repeat it per row, only the directory path encodes it.

## Error handling

- Malformed/undecodable ticks: unchanged, handled upstream in `session.rs`
  (skip and continue).
- Write failure: rows retained in the hot buffer, retried on next flush.
- Compaction failure: original small files left untouched, retried on next
  compactor tick (see Compactor above).
- Query/join failure (bad SQL, Polars error): returned as `Err` to the
  caller, not swallowed.

## Testing

- `batcher`: unchanged from the original design (flush at `max_rows`, flush
  after `max_age`, `tokio::time::pause()` for determinism).
- `writer`: write a small `Vec<Trade>`/`Vec<Quote>` to a temp dir, read the
  resulting parquet file back, assert rows/columns round-trip; assert the
  catalog gained the new path.
- `catalog` + `compactor`: seed a partition with several small files via the
  writer, run one compactor pass, assert: exactly one merged file remains
  (plus the excluded most-recent one), the merged file's rows equal the
  concatenation of the originals in order, and a concurrent catalog read
  during the swap never observes a deleted-but-still-listed path.
- `query`/`asof_join`: flush some rows to parquet, leave others only in the
  hot buffer, run a query and an `asof_join`; assert both see hot+cold data
  together, and that `asof_join` correctly pairs each trade with the latest
  prior quote (including the "no quote yet" `None` case).
- End-to-end within `tick_store`: push `Tick`s through the public API
  (bypassing FIX/network entirely), run a query and an `asof_join`, assert
  the expected rows come back.

## Open items for the implementation plan

- Exact `max_rows`/`max_age` defaults (proposed: 500-1000 rows / 1-2s).
- Exact `compact_interval`/`compact_threshold` defaults (proposed: 5
  minutes / 5 files — tune once running against real traffic; the
  right balance is "few enough files that scans stay cheap" vs. "compactor
  doesn't spend all its time re-merging").
- Confirm the pinned Polars version's `join_asof` and `SQLContext` feature
  coverage (e.g. `strategy=Backward` support, `set_sorted` behavior) before
  relying on them in the implementation plan.
- Whether the parquet writer (and the compactor's merged-file write) need
  an explicit write-then-rename for atomicity, or Polars'/the underlying
  parquet writer already guarantees the file doesn't appear until closed.
- Where `BatchConfig`/`data_dir` are supplied from in `fook_kdb::main` (a
  new YAML config file, reusing the `LogOnConfig` loading pattern, or CLI
  args).
