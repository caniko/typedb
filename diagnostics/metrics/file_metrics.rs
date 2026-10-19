/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use crate::{error_with_report, metrics::HistogramMetrics};

#[derive(Debug, Clone)]
pub struct FsyncMetrics {
    data: Option<FsyncMetricData>,
}

#[derive(Debug, Clone)]
struct FsyncMetricData {
    fsync_histogram: Arc<HistogramMetrics>,
    bytes_counter: Arc<AtomicU64>,
}

impl Default for FsyncMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl FsyncMetrics {
    pub fn new() -> Self {
        let data = FsyncMetricData {
            fsync_histogram: Arc::new(HistogramMetrics::new_duration()),
            bytes_counter: Arc::new(AtomicU64::new(0)),
        };
        Self { data: Some(data) }
    }

    pub fn disabled() -> Self {
        Self { data: None }
    }

    pub fn record_fsync_duration(&self, duration: Duration) {
        if let Some(data) = &self.data {
            data.fsync_histogram.observe_duration(duration);
        }
    }

    pub fn record_bytes_written(&self, bytes: u64) {
        if let Some(data) = &self.data {
            data.bytes_counter.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    pub fn fsync_histogram_snapshot(&self) -> crate::metrics::HistogramSnapshot {
        match &self.data {
            Some(data) => data.fsync_histogram.snapshot(),
            None => {
                error_with_report!("Fsync histogram snapshot requested when Fsync metrics are disabled");
                HistogramMetrics::new_duration().snapshot()
            }
        }
    }

    pub fn bytes_written(&self) -> u64 {
        match &self.data {
            Some(data) => data.bytes_counter.load(Ordering::Relaxed),
            None => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::FsyncMetrics;

    #[test]
    fn default_metrics_are_enabled_and_clones_share_observations() {
        for metrics in [FsyncMetrics::new(), FsyncMetrics::default()] {
            assert_eq!(metrics.bytes_written(), 0);
            assert_eq!(metrics.fsync_histogram_snapshot().count, 0);
            let cloned = metrics.clone();
            cloned.record_bytes_written(4096);
            cloned.record_fsync_duration(Duration::from_millis(2));
            assert_eq!(metrics.bytes_written(), 4096);
            let snapshot = metrics.fsync_histogram_snapshot();
            assert_eq!(snapshot.count, 1);
            assert_eq!(snapshot.sum, 2_000_000);
        }
    }
}
