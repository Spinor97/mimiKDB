use std::sync::{Arc, RwLock};

use tokio::time::Interval;

use crate::catalog::Catalog;

pub struct Compactor {
    catalog: Arc<RwLock<Catalog>>,
    interval: Interval,
    threshold: usize,
}

impl Compactor {
    pub fn new(interval: Interval, catalog: Arc<RwLock<Catalog>>, threshold: usize) -> Self {
        Self {
            catalog,
            interval,
            threshold,
        }
    }

    pub fn start(mut self) {
        tokio::spawn(async move {
            loop {
                self.interval.tick().await;

                let due = match self.catalog.read() {
                    Ok(guard) => guard.partitions_over(self.threshold),
                    Err(e) => {
                        eprintln!("catalog lock poisoned: {e}");
                        continue;
                    }
                };

                for (symbol, kind, date) in due {
                    let result = match self.catalog.write() {
                        Ok(mut guard) => guard.concat_from_cache(symbol.clone(), kind, date),
                        Err(e) => {
                            eprintln!("catalog lock poisoned: {e}");
                            continue;
                        }
                    };
                    if let Err(e) = result {
                        eprintln!("failed to compact {symbol} on {date}: {e}");
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use polars::prelude::*;
    use tempfile::tempdir;
    use tokio::time::{self, Duration};

    use super::*;
    use crate::catalog::Kind;

    fn write_test_parquet(path: &PathBuf, values: &[i64]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut df = DataFrame::new(values.len(), vec![Series::new("x".into(), values).into()]).unwrap();
        let file = std::fs::File::create(path).unwrap();
        polars::io::parquet::write::ParquetWriter::new(file)
            .finish(&mut df)
            .unwrap();
    }

    // Real time, not tokio::time::pause()/advance() -- pausing requires the
    // current_thread runtime flavor, but Polars' sink()/collect() (used by
    // every write in this pipeline, including concat_from_cache's merge)
    // requires a multi-threaded one. Those two are mutually exclusive, so
    // these use a short real interval and a real short wait instead.
    #[tokio::test(flavor = "multi_thread")]
    async fn compacts_partitions_over_threshold_on_the_next_tick() {
        let dir = tempdir().unwrap();
        let catalog = Arc::new(RwLock::new(Catalog::new(dir.path().to_path_buf())));
        let symbol: Arc<str> = Arc::from("BTC-USD");
        let date = chrono::Utc::now().date_naive();

        {
            let mut guard = catalog.write().unwrap();
            for i in 0..3 {
                let path = dir.path().join(format!("part-{i}.parquet"));
                write_test_parquet(&path, &[i as i64]);
                guard.add_file(symbol.clone(), Kind::Quote, date, path);
            }
        }

        // threshold=1: 3 files is over threshold, should get merged to 1
        let compactor = Compactor::new(time::interval(Duration::from_millis(10)), catalog.clone(), 1);
        compactor.start();

        time::sleep(Duration::from_millis(200)).await;

        let guard = catalog.read().unwrap();
        let files = guard.files_for(symbol, Kind::Quote, date);
        assert_eq!(files.len(), 1, "3 files over threshold 1 should have been merged into 1");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn leaves_partitions_at_or_under_threshold_alone() {
        let dir = tempdir().unwrap();
        let catalog = Arc::new(RwLock::new(Catalog::new(dir.path().to_path_buf())));
        let symbol: Arc<str> = Arc::from("BTC-USD");
        let date = chrono::Utc::now().date_naive();

        {
            let mut guard = catalog.write().unwrap();
            let path = dir.path().join("part-0.parquet");
            write_test_parquet(&path, &[1]);
            guard.add_file(symbol.clone(), Kind::Quote, date, path);
        }

        // threshold=1: exactly 1 file is not over threshold, should be left alone
        let compactor = Compactor::new(time::interval(Duration::from_millis(10)), catalog.clone(), 1);
        compactor.start();

        time::sleep(Duration::from_millis(200)).await;

        let guard = catalog.read().unwrap();
        let files = guard.files_for(symbol, Kind::Quote, date);
        assert_eq!(files.len(), 1, "single file at the threshold should not be touched");
    }
}
