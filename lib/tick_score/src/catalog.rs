use std::{collections::HashMap, path::PathBuf, sync::Arc};

use chrono::NaiveDate;
use polars::{error::PolarsResult, lazy::{dsl::{UnionArgs, concat}, frame::{LazyFrame, ScanArgsParquet}}, prelude::PlRefPath};

use crate::batcher::{get_output_path, stream_saving};

#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub enum Kind {
    Quote = 0,
    Trade = 1,
}

impl Kind {
    pub fn get_label(&self) -> &str {
        match self {
            Kind::Quote => "Quote",
            Kind::Trade => "Trade",
        }
    }
}

pub struct Catalog {
    data_dir: PathBuf,
    merged_idx: usize,
    partitions: HashMap<(Arc<str>, Kind, NaiveDate), Vec<PathBuf>>,
}

impl Catalog {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            merged_idx: 0,
            partitions: HashMap::new(),
        }
    }

    /// The root directory this catalog's files are written under. Callers
    /// that also need to write files directly (the batcher's flush path)
    /// should derive their target path from this rather than tracking
    /// their own copy of the same value -- two independently-configured
    /// data_dirs that happen to disagree is exactly the kind of bug that's
    /// easy to introduce and hard to notice.
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    #[inline(always)]
    fn take(&mut self, symbol: Arc<str>, kind: Kind, date: NaiveDate) -> Vec<PathBuf> {
        let Some(curr_path) = self.partitions.get_mut(&(symbol, kind, date)) else {
            return vec![];
        };

        std::mem::replace(curr_path, vec![])
    }

    #[inline(always)]
    pub fn add_file(&mut self, symbol: Arc<str>, kind: Kind, date: NaiveDate, path: PathBuf) {
        self.partitions.entry((symbol, kind, date)).or_default().push(path);
    }

    /// Files currently representing one partition, in write order -- what a
    /// reader (a query, an as-of join, this module's own tests) should scan
    /// to see that partition's data. Empty slice if the partition has no
    /// tracked files (never flushed, or nothing past `Catalog::new`).
    pub fn files_for(&self, symbol: Arc<str>, kind: Kind, date: NaiveDate) -> &[PathBuf] {
        self.partitions
            .get(&(symbol, kind, date))
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// All files currently tracked for one `Kind`, across every symbol and
    /// date -- what a general "SELECT * FROM trades" (or quotes) query
    /// scans. Cold data only: rows still sitting in a hot buffer that
    /// hasn't flushed yet aren't included here.
    pub fn all_files(&self, kind: Kind) -> Vec<PathBuf> {
        self.partitions
            .iter()
            .filter(|((_, k, _), _)| *k == kind)
            .flat_map(|(_, files)| files.iter().cloned())
            .collect()
    }

    /// All files currently tracked for one `(symbol, kind)`, across every
    /// date -- what a symbol-scoped query scans. Unlike [`Self::all_files`],
    /// this never touches any other symbol's files at all: the lookup is
    /// scoped before any file is even opened, not filtered afterward.
    pub fn files_for_symbol(&self, symbol: &Arc<str>, kind: Kind) -> Vec<PathBuf> {
        self.partitions
            .iter()
            .filter(|((s, k, _), _)| s == symbol && *k == kind)
            .flat_map(|(_, files)| files.iter().cloned())
            .collect()
    }

    /// Same as [`Self::files_for_symbol`], but further scoped to dates in
    /// `start..=end` (inclusive both ends) -- what a query with an explicit
    /// or defaulted date window scans, without touching any file outside
    /// that window.
    pub fn files_for_symbol_in_range(&self, symbol: &Arc<str>, kind: Kind, start: NaiveDate, end: NaiveDate) -> Vec<PathBuf> {
        self.partitions
            .iter()
            .filter(|((s, k, d), _)| s == symbol && *k == kind && *d >= start && *d <= end)
            .flat_map(|(_, files)| files.iter().cloned())
            .collect()
    }

    /// Partition keys currently tracking more than `threshold` files --
    /// what the compactor should merge on its next pass. Returns owned
    /// keys (cheap: Arc<str> clone is a refcount bump, Kind/NaiveDate are
    /// Copy) so the caller can act on each one without holding a borrow
    /// into `self` while it does.
    pub fn partitions_over(&self, threshold: usize) -> Vec<(Arc<str>, Kind, NaiveDate)> {
        self.partitions
            .iter()
            .filter(|(_, files)| files.len() > threshold)
            .map(|(key, _)| key.clone())
            .collect()
    }

    pub fn concat_from_cache(&mut self, symbol: Arc<str>, kind: Kind, date: NaiveDate) -> PolarsResult<()> {
        let key = (symbol.clone(), kind, date);
        // Takes ownership up front (no clone), which means every early
        // return below must put curr_path back before it returns Err --
        // otherwise a failed merge would leave the catalog thinking this
        // partition has no files, when the originals are still sitting on
        // disk untouched.
        let curr_path = self.take(symbol.clone(), kind, date);
        if curr_path.is_empty() {
            return Ok(());
        }

        let data: PolarsResult<Vec<LazyFrame>> = curr_path
            .iter()
            .map(|file| {
                let pl_path = PlRefPath::try_from_path(file)?;
                LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())
            })
            .collect();

        let data = match data {
            Ok(data) => data,
            Err(e) => {
                self.partitions.insert(key, curr_path);
                return Err(e);
            }
        };

        let union_args = UnionArgs {
            parallel: true,
            ..Default::default()
        };
        let lf = match concat(&data, union_args) {
            Ok(lf) => lf,
            Err(e) => {
                self.partitions.insert(key, curr_path);
                return Err(e);
            }
        };

        let save_path = get_output_path(&self.data_dir, kind, symbol.clone(), &format!("merged-{}.parquet", self.merged_idx), &date);
        self.merged_idx += 1;

        // Only once the merged file is confirmed written (stream_saving
        // propagates the actual write's result, not just plan-building
        // errors) do we swap the catalog to point at it, and only after
        // that swap do we remove the files it superseded.
        if let Err(e) = stream_saving(lf, &save_path) {
            self.partitions.insert(key, curr_path);
            return Err(e);
        }

        self.partitions.insert(key, vec![save_path]);
        remove_merged_files(&curr_path);

        Ok(())
    }
}

/// Deletes the small files a successful compaction has just superseded.
/// Only ever called after the merged file is durably written and the
/// catalog already points at it in place of these -- deleting first, or on
/// a failed merge, would destroy data with no remaining copy of it.
fn remove_merged_files(paths: &[PathBuf]) {
    for path in paths {
        if let Err(e) = std::fs::remove_file(path) {
            eprintln!("failed to remove superseded parquet file {path:?}: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;
    use tempfile::tempdir;

    fn write_test_parquet(path: &PathBuf, values: &[i64]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut df = DataFrame::new(values.len(), vec![Series::new("x".into(), values).into()]).unwrap();
        let file = std::fs::File::create(path).unwrap();
        polars::io::parquet::write::ParquetWriter::new(file)
            .finish(&mut df)
            .unwrap();
    }

    fn read_column_x(path: &PathBuf) -> Vec<i64> {
        LazyFrame::scan_parquet(
            PlRefPath::try_from_path(path).unwrap(),
            ScanArgsParquet::default(),
        )
        .unwrap()
        .collect()
        .unwrap()
        .column("x")
        .unwrap()
        .i64()
        .unwrap()
        .iter()
        .map(|v| v.unwrap())
        .collect()
    }

    #[test]
    fn add_file_and_partitions_over_threshold() {
        let mut catalog = Catalog::new(PathBuf::from("unused"));
        let symbol: Arc<str> = Arc::from("BTC-USD");
        let date = chrono::Utc::now().date_naive();

        assert!(catalog.partitions_over(0).is_empty());

        catalog.add_file(symbol.clone(), Kind::Quote, date, PathBuf::from("a.parquet"));
        catalog.add_file(symbol.clone(), Kind::Quote, date, PathBuf::from("b.parquet"));

        assert!(catalog.partitions_over(2).is_empty(), "exactly 2 files is not over a threshold of 2");
        assert_eq!(catalog.partitions_over(1), vec![(symbol, Kind::Quote, date)]);
    }

    #[test]
    fn files_for_returns_empty_slice_for_unknown_partition() {
        let catalog = Catalog::new(PathBuf::from("unused"));
        let symbol: Arc<str> = Arc::from("BTC-USD");
        let date = chrono::Utc::now().date_naive();

        assert!(catalog.files_for(symbol, Kind::Quote, date).is_empty());
    }

    #[test]
    fn concat_from_cache_merges_updates_catalog_and_deletes_originals() {
        let dir = tempdir().unwrap();
        let mut catalog = Catalog::new(dir.path().to_path_buf());
        let symbol: Arc<str> = Arc::from("BTC-USD");
        let date = chrono::Utc::now().date_naive();

        let f1 = dir.path().join("part-0.parquet");
        let f2 = dir.path().join("part-1.parquet");
        write_test_parquet(&f1, &[1, 2]);
        write_test_parquet(&f2, &[3, 4]);
        catalog.add_file(symbol.clone(), Kind::Quote, date, f1.clone());
        catalog.add_file(symbol.clone(), Kind::Quote, date, f2.clone());

        catalog
            .concat_from_cache(symbol.clone(), Kind::Quote, date)
            .expect("merge should succeed");

        // catalog now points at exactly one file
        let files = catalog.files_for(symbol.clone(), Kind::Quote, date);
        assert_eq!(files.len(), 1);
        let merged_path = files[0].clone();

        // the originals are gone, the merged file exists with all 4 rows
        assert!(!f1.exists(), "original small file should be deleted after a successful merge");
        assert!(!f2.exists(), "original small file should be deleted after a successful merge");
        assert!(merged_path.exists());

        let mut merged_values = read_column_x(&merged_path);
        merged_values.sort();
        assert_eq!(merged_values, vec![1, 2, 3, 4]);
    }

    #[test]
    fn concat_from_cache_restores_the_catalog_when_a_source_file_is_missing() {
        let dir = tempdir().unwrap();
        let mut catalog = Catalog::new(dir.path().to_path_buf());
        let symbol: Arc<str> = Arc::from("BTC-USD");
        let date = chrono::Utc::now().date_naive();

        let real = dir.path().join("part-0.parquet");
        let missing = dir.path().join("does-not-exist.parquet");
        write_test_parquet(&real, &[1]);
        catalog.add_file(symbol.clone(), Kind::Quote, date, real.clone());
        catalog.add_file(symbol.clone(), Kind::Quote, date, missing.clone());

        let result = catalog.concat_from_cache(symbol.clone(), Kind::Quote, date);
        assert!(result.is_err(), "merge should fail: one of the source files doesn't exist");

        // the catalog must still point at exactly the original two files --
        // neither lost nor replaced by a (nonexistent) merged file
        let mut files = catalog.files_for(symbol, Kind::Quote, date).to_vec();
        files.sort();
        let mut expected = vec![real, missing];
        expected.sort();
        assert_eq!(files, expected);
    }
}